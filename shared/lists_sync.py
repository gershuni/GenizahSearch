# -*- coding: utf-8 -*-
"""
Lists Cloud Sync Module

Provides bidirectional sync between local ListsManager (pickle file)
and Supabase cloud storage. Enables cross-device sync of user lists
between the desktop app and web app.

Part of Phase 5: Desktop App Supabase Migration
"""
import collections
import json
import logging
import threading
import time
from typing import Optional, Dict, Any

from shared.local_sys_id import is_local_sys_id

logger = logging.getLogger(__name__)

# Try to import Supabase
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    Client = None

try:
    from postgrest.exceptions import APIError
except ImportError:  # pragma: no cover - postgrest ships with supabase
    class APIError(Exception):
        code = None

# Configuration - centralized via provider
from shared.supabase_provider import get_url, get_anon_key
SUPABASE_URL = get_url()
SUPABASE_ANON_KEY = get_anon_key()

# English text, translated where it is shown (the desktop sync dialog).
DOWNLOAD_BACKUP_FAILED = (
    "Your lists on this computer could not be backed up, so nothing was downloaded. "
    "Check that the data folder can be written to, then try again."
)
# Formatted with (items_pushed, items_failed); the dialog formats the
# translation from the same two counts.
UPLOAD_PARTLY_FAILED = "Uploaded {} item(s), but {} failed to upload to the cloud."

# Rows are read in pages by row id; the next page starts after the last id returned,
# and only an empty page ends a read, because the server may cap a page below PAGE_SIZE.
PAGE_SIZE = 1000
ROW_COLUMNS = 'id, list_id, sys_id, fl_id, note, tags, shelfmark'
# The confirmation reads a row's identity too, so a row it finds is written to as a read one is.
CONFIRM_COLUMNS = 'id, list_id, sys_id, fl_id, note, tags, shelfmark'
CONFIRM_CHUNK = 200
# Updates whose only change is the page, per upload (the first upload after the
# page column appears would otherwise send one per entry).
PAGE_BACKFILL_PER_PASS = 200
# Label of the line between the desktop's note and the cloud's, translated when merged.
NOTE_MARK = "from the cloud"
# The only keys of the local store an upload may change.
IDENTITY_FIELDS = {
    'store': ('cloud_account', 'cloud_deletes'),
    'projects': ('cloud_id',),
    'lists': ('cloud_id', 'list_state_unsent', 'list_name_unsent'),
    'items': ('cloud_id', 'cloud_rows'),
}
# The list a row is in when reading it again failed: equal to no list id.
UNKNOWN_LIST = object()
# On a list whose own state (Trash, colour, project) was changed on this computer
# (ListsManager marks each such change), or that took its cloud list in a download
# while holding no cloud id and whose state differs from that cloud list's: the local
# value stands until an upload has sent it -- a download before then keeps it rather
# than importing the cloud's, as for a rename (LIST_NAME_UNSENT) -- so both orders of
# a Download and an Upload end alike. Its value names the parts held back (a list of
# LIST_STATE_FIELDS), or True for all three (the mark before it named them); the cloud's
# value of every other part is still taken. It says nothing about the name: a download
# still takes a website rename unless LIST_NAME_UNSENT holds it back.
LIST_STATE_UNSENT = 'list_state_unsent'
LIST_STATE_FIELDS = ('color', 'project', 'trash')


def _held_state(list_data):
    """The parts of a list's own state its LIST_STATE_UNSENT mark holds back (a set of LIST_STATE_FIELDS)."""
    mark = list_data.get(LIST_STATE_UNSENT)
    if not mark:
        return set()
    if isinstance(mark, (list, tuple, set, frozenset)):
        return set(mark) & set(LIST_STATE_FIELDS)
    return set(LIST_STATE_FIELDS)
# On a list renamed on this computer (ListsManager.update_list sets it, and nothing
# else does) until an upload's write of that name returned the row: the new name
# stands over the cloud list's at a download, and only an upload of such a list
# writes a name to an existing cloud list. Colour, project and Trash state still
# come from the cloud list at a download.
LIST_NAME_UNSENT = 'list_name_unsent'


# ---------------------------------------------------------------------------
# Identity of an entry: (sys_id, fl_id, page)
# ---------------------------------------------------------------------------

def _norm(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _id_key(value):
    return (0, value, '') if isinstance(value, int) else (1, 0, str(value))


def _after(query, after):
    """One page of a keyset read (ListsCloudSync._paged): the rows after row id `after`.

    In id order and at most PAGE_SIZE rows; no id filter when `after` is None (the first page).
    """
    if after is not None:
        query = query.gt('id', after)
    return query.order('id').limit(PAGE_SIZE)


def _item_identity(item_id, item):
    """(sys_id, fl_id, page) of a local item: its fields, else the ::fl:: / ::img:: parts of its key."""
    key = str(item_id)
    page = _norm(item.get('img'))
    if page is None and '::img::' in key:
        page = _norm(key.split('::img::', 1)[1].split('::', 1)[0])
    fl_id = _norm(item.get('fl_id'))
    if fl_id is None and '::fl::' in key:
        fl_id = _norm(key.split('::fl::', 1)[1].split('::', 1)[0])
    sys_id = item.get('sys_id') or key.split('::', 1)[0]
    return str(sys_id), fl_id, page


def _row_identity(row, has_page):
    sys_id = row.get('sys_id')
    return (None if sys_id is None else str(sys_id), _norm(row.get('fl_id')),
            _norm(row.get('page')) if has_page else None)


def _same_entry(a, b, has_page):
    """True only when two identities are certainly one entry.

    Same sys_id; no conflict (two different non-empty fl_ids, or -- with the page
    column -- two different non-empty pages); and one anchor: the same non-empty
    fl_id, or with the page column the same non-empty page, or both being the
    whole manuscript (neither fl_id nor page). Without the column only the fl_id
    anchor exists: a page and the whole manuscript look alike there.
    """
    if a[0] is None or a[0] != b[0]:
        return False
    if a[1] and b[1] and a[1] != b[1]:
        return False
    if has_page and a[2] and b[2] and a[2] != b[2]:
        return False
    if a[1] and a[1] == b[1]:
        return True
    if has_page and a[2] and a[2] == b[2]:
        return True
    return bool(has_page and not a[1] and not b[1] and not a[2] and not b[2])


# ---------------------------------------------------------------------------
# Notes and tags
# ---------------------------------------------------------------------------

def _clean(text):
    return (text or '').replace('\r\n', '\n').rstrip()


def _has_lines(big, small):
    """small is empty, equal to big, or a run of complete lines of big (never a raw substring)."""
    if not small or small == big:
        return True
    return ("\n" + small + "\n") in ("\n" + big + "\n")


def _separators():
    """The line kept notes are joined with, in English and in Hebrew (a note may hold either)."""
    labels = [NOTE_MARK]
    try:
        from shared.genizah_translations import TRANSLATIONS  # noqa: PLC0415 - loaded on first merge only
        labels.append(TRANSLATIONS.get(NOTE_MARK, NOTE_MARK))
    except Exception:
        pass
    return ["\n\n--- " + label + " ---\n" for label in dict.fromkeys(labels)]


def _blocks(text):
    """The texts a kept note is made of: the parts between its marker lines."""
    parts = [text]
    for sep in _separators():
        parts = [piece for part in parts for piece in part.split(sep)]
    return [p.rstrip() for p in parts if p.strip()]


def _holds(big, small):
    """big already contains small: as complete lines, or every text small was kept from."""
    big, small = _clean(big), _clean(small)
    if _has_lines(big, small):
        return True
    parts = _blocks(small)
    return len(parts) > 1 and all(_has_lines(big, part) for part in parts)


def _same_text(a, b):
    """Equal, or each holds the other: the same notes kept in another order."""
    return (a or '') == (b or '') or (_holds(a, b) and _holds(b, a))


def _keep_both(local, cloud):
    """Both texts: the desktop's, a marker line, then what the cloud's text adds to it."""
    local, cloud = local or '', cloud or ''
    if _holds(local, cloud):
        return local
    if _holds(cloud, local):
        return cloud
    from shared.lists_manager import _tr  # noqa: PLC0415 - GUARD-01: no module-level genizah_core
    sep = "\n\n--- " + _tr(NOTE_MARK) + " ---\n"
    # A cloud text that is itself a kept note adds only the texts this one lacks, so two
    # computers that kept the same notes in another order converge instead of nesting them.
    parts = _blocks(cloud)
    missing = [part for part in parts if not _has_lines(_clean(local), part)]
    added = sep.join(missing) if len(parts) > 1 else cloud
    return local + sep + added


def _fold_notes(texts):
    out = texts[0]
    for text in texts[1:]:
        out = _keep_both(out, text)
    return out


def _union(first, other):
    out = list(first or [])
    for tag in other or []:
        if tag not in out:
            out.append(tag)
    return out


def _tagset(tags):
    return set(json.dumps(t, ensure_ascii=False, sort_keys=True) for t in (tags or []))


def _tags_filter_literal(tags):
    """list_items.tags is jsonb: the cs/cd filters take JSON text, never the text[] literal {a,b}."""
    return json.dumps(tags, ensure_ascii=False, separators=(',', ':'))


def count_differing_notes(store):
    """Entries whose note or tags differ from the account's copy and were left as they are.

    One per local item (however many of its lists say so) with a current membership
    whose record says the cloud's note or tags differed: the item is still in that
    list, and the list is one a pass reaches -- it exists, is synced, and is not in
    the Trash (restored, its entries count again). A moved row kept because the
    website edited it (a '~' record with 'differs') counts too: a Download folds its
    text into the entry wherever the row is.
    """
    if not store.get('cloud_account'):
        return 0
    # copies of the store's dicts and lists: a direct upload may run while the user edits
    lists = dict(store.get('lists') or {})

    def reached(key):
        ld = lists.get(key)
        return isinstance(ld, dict) and _syncable_list(key, ld) and not ld.get('deleted_at')

    n = 0
    for item in list((store.get('items') or {}).values()):
        in_lists = list(item.get('lists') or [])
        if any(not rec.get('gone') and rec.get('differs')
               and ((key in in_lists and reached(key)) or _is_move_key(key))
               for key, rec in _records(item)):
            n += 1
    return n


def has_unsent_changes(store, user_id=None):
    """Whether the store holds a change the next upload of this account would send.

    Read from the store alone (no network), so changes saved in lists.pkl -- made while
    offline, or left by an upload that did not finish -- count as unsent after a restart.
    One pass over the store: the desktop asks it once when its list-sync runner is made
    and at each log-in, never per edit. True when any of:
    - a removal of this account waits for its delete (store['cloud_deletes']);
    - a list carries LIST_STATE_UNSENT or LIST_NAME_UNSENT, or a list or project has no
      cloud id yet (the upload creates it);
    - an entry is in a list whose entries the upload writes (synced, not in the Trash)
      with no record of its row for this account -- a row the website removed (a 'gone'
      record, waiting for the user's answer) is not re-sent, so it does not count;
    - a record's note or tags differ from the entry's (what this computer last agreed
      with the account), where the record knows them and did not keep a differing note
      for Merge Both ('differs': the upload leaves those as they are);
    - a moved row waits to follow its entry, into no list or into one the upload writes
      (a move into a list in the Trash waits for Restore).
    My Library entries never count: they are never sent. user_id None: the account the
    store was last synced with.
    """
    account = user_id if user_id is not None else store.get('cloud_account')
    lists = dict(store.get('lists') or {})

    def written(lid):
        ld = lists.get(lid)
        return isinstance(ld, dict) and _syncable_list(lid, ld) and not ld.get('deleted_at')

    if any(isinstance(entry, dict) and entry.get('account') == account
           for entry in list((store.get('cloud_deletes') or {}).values())):
        return True
    for lid, ld in lists.items():
        if isinstance(ld, dict) and _syncable_list(lid, ld) and (
                ld.get(LIST_STATE_UNSENT) or ld.get(LIST_NAME_UNSENT) or ld.get('cloud_id') is None):
            return True
    if any(isinstance(pd, dict) and pd.get('cloud_id') is None
           for pd in list((store.get('projects') or {}).values())):
        return True
    own = account is not None and store.get('cloud_account') == account   # its records are this account's
    for iid, it in list((store.get('items') or {}).items()):
        if not isinstance(it, dict) or is_local_sys_id(it.get('sys_id', iid)):
            continue
        in_lists = [lid for lid in list(it.get('lists') or []) if written(lid)]
        if not own:
            if in_lists:
                return True
            continue
        recs = it.get('cloud_rows') if isinstance(it.get('cloud_rows'), dict) else {}
        for lid in in_lists:
            rec = recs.get(lid)
            if isinstance(rec, dict) and rec.get('gone'):
                continue
            if _live_record(it, lid) is None:
                return True
            if rec.get('differs'):
                continue
            if 'note' in rec and not _same_text(it.get('note') or '', rec.get('note') or ''):
                return True
            if 'tags' in rec and _tagset(it.get('tags')) != _tagset(rec.get('tags')):
                return True
        for _, rec in _orphans(it):
            if not rec.get('differs') and (rec.get('to') is None or written(rec.get('to'))):
                return True
    return False


def remembered_ids(store, user_id):
    """The rows a download confirms: every non-gone record's, while the store is this account's."""
    if store.get('cloud_account') not in (None, user_id):
        return []
    return [rec['id'] for it in list((store.get('items') or {}).values())
            for _, rec in _records(it) if not rec.get('gone') and rec.get('id') is not None]


# ---------------------------------------------------------------------------
# Records: item['cloud_rows'] = {local_list_id: {'id', 'list', 'note'?, 'tags'?, 'gone'?, 'differs'?}}
# A row whose entry was moved here is kept under '~<row id>' with 'to': the local list
# it is bound for (shared/lists_manager.py writes it); store['cloud_deletes'] holds the
# rows of explicit removals, {str(row id): {'id', 'list', 'account'}}, until they go.
# ---------------------------------------------------------------------------

def _records(item):
    return [(k, r) for k, r in (item.get('cloud_rows') or {}).items() if isinstance(r, dict)]


def _is_move_key(key):
    return isinstance(key, str) and key.startswith('~')


def _live_record(item, list_key):
    rec = (item.get('cloud_rows') or {}).get(list_key)
    if isinstance(rec, dict) and not rec.get('gone') and rec.get('id') is not None:
        return rec
    return None


def _orphans(item):
    """Records for local lists the item is no longer in (a Move, a removal, a merged list)."""
    lists = list(item.get('lists') or [])
    return [(k, r) for k, r in _records(item)
            if k not in lists and not r.get('gone') and r.get('id') is not None]


def _orphan_ids(items):
    return {r['id'] for it in list(items.values()) for _, r in _orphans(it)}


def _base_of(item, row_id):
    """A copy of the record naming this row (for its note/tag bases), or None."""
    for _, rec in _records(item):
        if not rec.get('gone') and rec.get('id') == row_id:
            return dict(rec)
    return None


def _drop_record(item, list_key):
    recs = item.get('cloud_rows')
    if recs:
        recs.pop(list_key, None)
        if not recs:
            item.pop('cloud_rows', None)


def _drop_left_tombstones(items):
    for item in list(items.values()):
        lists = list(item.get('lists') or [])
        for key, rec in _records(item):
            if rec.get('gone') and key not in lists:
                _drop_record(item, key)


def rows_index(store):
    """Which records name each row: {row id: [(item, list key)]}, for record_row."""
    named = collections.defaultdict(list)
    for it in list((store.get('items') or {}).values()):
        for key, rec in _records(it):
            if rec.get('id') is not None:
                named[rec['id']].append((it, key))
    return named


def record_row(store, user_id, item_id, list_key, row_id, cloud_list_id, note=None, tags=None, differs=False,
               named=None, item=None):
    """The only writer of a record. note/tags: the cloud values the local copy is known to include.

    Returns True when it wrote. named: rows_index(store), kept up to date here (built when
    None). The row is this membership's now: another record naming it (this item's for
    another list, or another item's that the row has left) is stale, and a removal of it
    this account was waiting to send is over -- the row went to a membership again.
    """
    if store.get('cloud_account') != user_id:
        logger.error("Not recording a cloud row: the lists belong to another account")
        return False
    if item is None:
        item = (store.get('items') or {}).get(item_id)
        if not isinstance(item, dict):
            return False
    if named is None:
        named = rows_index(store)
    keep = []
    for other_item, key in named.get(row_id, ()):
        if other_item is item and key == list_key:
            continue
        other = (other_item.get('cloud_rows') or {}).get(key)
        if not isinstance(other, dict) or other.get('id') != row_id:
            continue                       # that record names another row by now
        if other.get('gone'):
            keep.append((other_item, key))
        else:
            _drop_record(other_item, key)
    rec = {'id': row_id, 'list': cloud_list_id}
    if note is not None:
        rec['note'] = note
    if tags is not None:
        rec['tags'] = list(tags)
    if differs:
        rec['differs'] = True
    item.setdefault('cloud_rows', {})[list_key] = rec
    named[row_id] = keep + [(item, list_key)]
    if item.get('cloud_id') is not None and item.get('cloud_id') == row_id:
        item.pop('cloud_id', None)
    pending = store.get('cloud_deletes')
    entry = pending.get(str(row_id)) if pending else None
    if isinstance(entry, dict) and entry.get('account') == user_id:
        del pending[str(row_id)]
        if not pending:
            store.pop('cloud_deletes', None)
        logger.info("Row %s is an entry's again; its removal is not sent", row_id)
    return True


def _remember(pass_, store, item_id, item, list_key, row_id, cloud_list_id, note=None, tags=None, differs=False):
    """record_row for a pass, which also tells the pass's on_recorded (an upload on a copy)."""
    if not record_row(store, pass_.user_id, item_id, list_key, row_id, cloud_list_id, note, tags, differs,
                      named=pass_.records_naming(store), item=item):
        return
    pass_.recorded.add((item_id, list_key))
    if pass_.report is not None:
        pass_.report(('row', item_id, list_key, row_id, cloud_list_id, note,
                      None if tags is None else list(tags), bool(differs)))


def _set_field(store, kind, local_id, key, value, report=None):
    """The one writer, in an upload, of a list's or project's cloud_id or a list's unsent marks.

    value None removes the key. A write that changes the store is told to report (an
    upload on a copy), as ('field', kind, local id, key, value), so a close can make it again.
    """
    target = (store.get(kind) or {}).get(local_id)
    if not isinstance(target, dict):
        return
    if value is None:
        if key not in target:
            return
        target.pop(key, None)
    else:
        if target.get(key) == value and key in target:
            return
        target[key] = value
    if report is not None:
        report(('field', kind, local_id, key, value))


def apply_account_guard(store, user_id):
    """Records belong to one account: drop another account's, then claim the store for this one.

    Another account's pending removals stay: they are sent when that account syncs again.
    """
    account = store.get('cloud_account')
    if account is not None and account != user_id:
        dropped = 0
        for item in list((store.get('items') or {}).values()):
            dropped += len(_records(item))
            item.pop('cloud_rows', None)
            item.pop('cloud_id', None)
        logger.info("Lists were last synced with another account: dropped %d cloud row record(s)", dropped)
    store['cloud_account'] = user_id


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

def _is_missing_column(exc, column):
    code = str(getattr(exc, 'code', '') or '')
    text = f"{getattr(exc, 'message', '') or ''} {exc}"
    if code in ('42703', 'PGRST204'):
        return column in text
    return column in text and ('does not exist' in text or 'schema cache' in text)


def _is_url_too_long(exc):
    return isinstance(exc, APIError) and str(getattr(exc, 'code', '')) in ('414', '431')


def _insert_rolled_back(exc):
    """True when an insert error proves nothing was written, so retrying rows one by one is safe.

    A PostgreSQL SQLSTATE (five digits and capitals), or a PostgREST code raised
    before any query runs. Anything else -- other PGRST codes, an HTTP status from
    a gateway, a transport error -- may follow a commit.
    """
    if not isinstance(exc, APIError):
        return False
    code = getattr(exc, 'code', None)
    if not isinstance(code, str):
        return False
    if len(code) == 5 and all(ch.isdigit() or 'A' <= ch <= 'Z' for ch in code):
        return True
    if code.startswith('PGRST') and code[5:].isdigit():
        n = int(code[5:])
        return 100 <= n <= 108 or 200 <= n <= 205 or 300 <= n <= 303
    return False


def _session_user(client):
    """The user id of the client's current session, or None (no session, or no way to tell)."""
    try:
        session = client.auth.get_session()
        user = getattr(session, 'user', None) if session is not None else None
        return str(user.id) if user is not None and getattr(user, 'id', None) is not None else None
    except Exception:
        return None


class _Stopped(BaseException):
    """should_stop() said so before a request. Not an Exception: no handler of a failed request takes it."""


class _Pass:
    """Everything one sync pass learns; never kept on the sync object."""

    def __init__(self, client, user_id, should_stop=None, withdrawn=None, report=None):
        self.client = client
        self.user_id = user_id
        self.should_stop = should_stop
        self.withdrawn = withdrawn      # () -> the memberships removed meanwhile: not inserted, not moved
        self.report = report            # on_recorded of an upload on a copy
        self.recorded = set()           # (item id, list key) given a record in this pass
        self.pending_rows = {}          # a download: {cloud list: rows this account waits to delete there}
        self.has_page = None
        self.backfill_left = PAGE_BACKFILL_PER_PASS
        self.auth_ok = False
        self.session_before = None
        self.where = {}          # row id -> (cloud list id, note, tags), from every read and the confirmation
        self.rows_by_id = {}     # row id -> the full row, from list reads and the confirmation
        self.confirmed_only = set()
        self.absent = set()
        self.locate_ok = True
        self.claimed = set()     # row ids claimed in this pass
        self.held_notes = set()  # items with a row whose note differs and may not be replaced
        self.held_tags = set()
        self.too_long = set()    # items a note or tag write of this pass was refused for (URL too long)
        self.named = None        # row id -> [(item, list key)] of the records naming it

    def check(self):
        """Called before every request: raises _Stopped once should_stop() is true."""
        if self.should_stop is not None and self.should_stop():
            raise _Stopped()

    def withdrawn_now(self):
        """The memberships the user removed while this pass ran (read right before a write is sent)."""
        if self.withdrawn is None:
            return frozenset()
        try:
            return frozenset(self.withdrawn())
        except Exception as e:
            logger.warning("Could not read the removals made during the upload: %s", e)
            return frozenset()

    def records_naming(self, store):
        """Which records name each row: built at the pass's first record, then kept by record_row.

        After it is built no record is made except by record_row; records dropped or
        changed since are checked again where an entry is used.
        """
        if self.named is None:
            self.named = rows_index(store)
        return self.named

    def prove_auth(self):
        after = _session_user(self.client)
        self.auth_ok = (self.session_before is not None and self.session_before == str(self.user_id)
                        and after == str(self.user_id))


def _list_order(store):
    lists = store.get('lists') or {}
    order = [lid for lid in list(store.get('lists_order') or []) if lid in lists]
    seen = set(order)
    order += [lid for lid in list(lists) if lid not in seen]
    return order


def _syncable_list(list_id, list_data):
    return list_id != 'recent' and not list_data.get('is_system')


# ---------------------------------------------------------------------------
# Matching one list's rows with local items
# ---------------------------------------------------------------------------

def _match_rows(rows, candidates, list_key, has_page, claimed, orphan_ids, used, content_ok=None):
    """Pair cloud rows of one list with local items. Returns {row_id: item_id}.

    A row is claimed once per pass (claimed, shared across lists); an item once per
    membership (used, shared across the cloud lists read for list_key). Rows that an
    orphan record names are left out: they are moved or folded, never paired. Steps: a remembered row id;
    the same (sys_id, fl_id, page) -- (sys_id, fl_id) without the page column; then
    rows that are certainly the same entry. content_ok(row), when given, says whether
    a row may be paired by content (steps 2 and 3) at all.
    """
    rows = sorted((r for r in rows if r.get('id') not in orphan_ids and r.get('id') not in claimed),
                  key=lambda r: _id_key(r.get('id')))
    by_id = {r['id']: r for r in rows}
    cands = sorted(candidates, key=lambda c: (list_key not in (c[1].get('lists') or []),
                                              c[1].get('added') or 0, str(c[0])))
    matched = {}

    def claim(rid, iid):
        matched[rid] = iid
        used.add(iid)
        claimed.add(rid)

    for iid, it in cands:
        if iid in used:
            continue
        ids = []
        own = _live_record(it, list_key)
        if own:
            ids.append(own['id'])
        ids += [r['id'] for k, r in _records(it) if k != list_key and not r.get('gone') and r.get('id') is not None]
        for rid in ids:
            if rid in by_id and rid not in claimed:
                claim(rid, iid)
                break

    def pick(pool, it, prefer_page=None):
        note = it.get('note') or ''
        return min(pool, key=lambda r: ((_norm(r.get('page')) != prefer_page) if prefer_page is not None else False,
                                        (r.get('note') or '') != note, _id_key(r['id'])))

    by_sys = collections.defaultdict(list)
    for r in rows:
        if r.get('sys_id') is not None:
            by_sys[str(r['sys_id'])].append(r)

    def free(r):
        return r['id'] not in claimed and (content_ok is None or content_ok(r))

    for iid, it in cands:
        if iid in used:
            continue
        me = _item_identity(iid, it)
        key = me if has_page else me[:2]
        pool = [r for r in by_sys.get(me[0], ()) if free(r)
                and (_row_identity(r, has_page) if has_page else _row_identity(r, has_page)[:2]) == key]
        if pool:
            claim(pick(pool, it)['id'], iid)
    for iid, it in cands:
        if iid in used:
            continue
        me = _item_identity(iid, it)
        pool = [r for r in by_sys.get(me[0], ()) if free(r)
                and _same_entry(me, _row_identity(r, has_page), has_page)]
        if pool:
            claim(pick(pool, it, prefer_page=me[2] if has_page else None)['id'], iid)
    return matched


class ListsCloudSync:
    """
    Handles synchronization between local ListsManager and Supabase.

    Sync strategy:
    - On login: Pull cloud lists and merge with local
    - On list/item changes: Push to cloud if logged in
    - Every membership of an entry in a list has its own cloud row; the desktop
      remembers which (item['cloud_rows']) and the note/tags it last agreed on
    - An upload replaces a cloud note or tag set only when only this computer
      changed it since then; a download keeps both differing notes (the cloud's
      under a marked line) and combines tags
    - An upload deletes only the rows this computer remembered for an entry the
      user removed here (store['cloud_deletes']), by id and list, and a moved row
      whose destination has its own row, only while the entry holds its text; rows
      the website removed are recorded, not re-uploaded, and wait for the user
    - My Library (LOCAL) entries are never sent or fetched

    Usage:
        sync = ListsCloudSync(lists_manager)
        sync.set_user(user_id)  # Call after login
        sync.sync_from_cloud()  # Pull cloud data
        sync.sync_to_cloud()    # Push local data
    """

    def __init__(self, lists_manager=None):
        """Initialize the sync manager."""
        self.lists_manager = lists_manager
        self._client: Optional[Client] = None
        self._user_id: Optional[str] = None
        self._last_sync: float = 0
        self._sync_lock = threading.Lock()  # one pass at a time
        self._external_client = None  # Can be set to use an authenticated client

    @property
    def _sync_in_progress(self):
        return self._sync_lock.locked()

    def set_client(self, client: Client):
        """Set an external authenticated client (from corrections system)."""
        self._external_client = client
        logger.debug("External authenticated client set for lists sync")

    def _get_client(self) -> Optional[Client]:
        """Get Supabase client - preferring the authenticated external client."""
        # Prefer external authenticated client
        if self._external_client:
            return self._external_client

        if not SUPABASE_AVAILABLE or not SUPABASE_ANON_KEY:
            return None

        if self._client is None:
            try:
                self._client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
            except Exception as e:
                logger.error(f"Failed to create Supabase client: {e}")
                return None
        return self._client

    def set_user(self, user_id: str):
        """Set the current user ID (UUID from Supabase auth)."""
        self._user_id = user_id

    def clear_user(self):
        """Clear user ID (on logout), and the client: no request can go out on the signed-out account's."""
        self._user_id = None
        self._external_client = None

    def is_sync_available(self) -> bool:
        """Check if cloud sync is available."""
        available = (
            SUPABASE_AVAILABLE and
            bool(SUPABASE_ANON_KEY) and
            bool(self._user_id) and
            self.lists_manager is not None
        )
        if not available:
            logger.debug(f"Sync not available: SUPABASE_AVAILABLE={SUPABASE_AVAILABLE}, "
                        f"ANON_KEY_SET={bool(SUPABASE_ANON_KEY)}, "
                        f"user_id={self._user_id is not None}, "
                        f"lists_manager={self.lists_manager is not None}")
        return available

    def get_cloud_lists_preview(self, should_stop=None) -> Dict[str, Any]:
        """
        Get a preview of cloud lists without syncing.
        Used to show user what will be synced before they decide.
        Network only; should_stop() is asked before each request.

        Returns:
            Dict with 'success', 'lists' (list of {name, color, item_count}), 'error'
            (and 'stopped' True when should_stop ended it)
        """
        logger.debug(f"get_cloud_lists_preview called, user_id={self._user_id}")
        if not self.is_sync_available():
            logger.warning("Cloud lists preview: sync not available")
            return {'success': False, 'lists': [], 'error': 'Sync not available'}

        def stopped():
            return should_stop is not None and should_stop()
        try:
            client = self._get_client()
            if not client:
                return {'success': False, 'lists': [], 'error': 'No Supabase client'}

            # Fetch user's lists from cloud
            logger.info(f"Querying user_lists for user_id={self._user_id}")
            if stopped():
                return {'success': False, 'lists': [], 'error': 'Sync stopped', 'stopped': True}
            lists_response = client.table('user_lists').select('*').eq(
                'user_id', self._user_id
            ).execute()
            logger.info(f"Got {len(lists_response.data or [])} lists from Supabase")

            cloud_lists = []
            for lst in lists_response.data or []:
                # Get item count for this list
                if stopped():
                    return {'success': False, 'lists': [], 'error': 'Sync stopped', 'stopped': True}
                items_response = client.table('list_items').select('id').eq(
                    'list_id', lst['id']
                ).execute()
                item_count = len(items_response.data or [])

                cloud_lists.append({
                    'id': lst['id'],
                    'name': lst.get('name', 'Unnamed'),
                    'color': lst.get('color', '#FFD700'),
                    'item_count': item_count,
                    'is_system': lst.get('is_system', False)
                })

            return {'success': True, 'lists': cloud_lists, 'error': None}

        except Exception as e:
            logger.error(f"Error getting cloud lists preview: {e}")
            return {'success': False, 'lists': [], 'error': str(e)}

    def _backup_local_data(self, direction: str) -> bool:
        """Snapshot the local lists before a sync. Returns True if the snapshot was written.

        One file per direction, lists.pkl.pre-download and lists.pkl.pre-upload,
        written from the in-memory store. A Merge runs the download and then the
        upload, so the upload's snapshot must not replace the download's: the
        download is the direction that rewrites local notes, tags and
        memberships, and .pre-download is the state from before it. (A save is
        not a fresh backup: ListsManager rotates .bak1-3 once per session.)
        """
        if not self.lists_manager:
            return False
        try:
            if not self.lists_manager.write_snapshot(f"pre-{direction}"):
                return False
            logger.info("Saved a snapshot of the local lists before the %s", direction)
            return True
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return False

    # ------------------------------------------------------------------
    # Account guard and reads
    # ------------------------------------------------------------------

    @staticmethod
    def _guard(store, pass_):
        """Records belong to one account: drop another account's, then claim the store for this one."""
        apply_account_guard(store, pass_.user_id)

    def _paged(self, pass_, build):
        """Read every row of a query, a page at a time by row id. Returns (rows, complete).

        build(after) is the query ordered by id and limited to PAGE_SIZE rows, with
        `.gt('id', after)` unless `after` is None (the first page) -- see _after. Each
        next page starts after the last id the page before returned (keyset paging;
        list_items.id and user_lists.id are SERIAL, supabase_setup.sql). The read ends
        at the first EMPTY page: a short page is not the end, because the server may
        answer fewer rows per request than PAGE_SIZE.

        complete: the read reached an empty page. Such a read returned every row that
        was there for the whole of it. A row added during the read has an id above
        every id returned so far, so a later page returns it; a row removed during the
        read may or may not be among those returned. No removal can move a later page
        past a row it has not returned (an offset can, and then equal counts and an
        equal number of ids prove nothing), so the read needs no count.

        Not complete: a page whose ids do not rise from the last id (a row the read
        already has, one before it, or rows out of order: the server did not apply
        the filter or the order). The read ends there, without that page. Any error
        raises.

        The gap accepted: an id is taken from the sequence when a row is inserted, not
        when its transaction commits. A row that took an id below one this read already
        passed, and committed only after the read passed it, is not returned. Rows are
        added by single-statement inserts (the website one row, a desktop a batch), so
        that window is milliseconds. (A row moved into the list during the read keeps
        its lower id and may be missed the same way -- as it would be, moved just after
        the read.)
        """
        rows, after = [], None
        while True:
            pass_.check()
            page = build(after).execute().data or []
            if not page:
                return rows, True
            ids = [row.get('id') for row in page]
            keys = ([] if after is None else [_id_key(after)]) + [_id_key(rid) for rid in ids]
            if None in ids or any(a >= b for a, b in zip(keys, keys[1:])):
                return rows, False   # ids that do not rise from the last one: see "Not complete"
            rows.extend(page)
            after = ids[-1]

    def _fetch_list_rows(self, pass_, cloud_list_id):
        client = pass_.client

        def reader(cols):
            return lambda after: _after(client.table('list_items').select(cols)
                                        .eq('list_id', cloud_list_id), after)

        if pass_.has_page is not False:
            try:
                rows, complete = self._paged(pass_, reader(ROW_COLUMNS + ', page'))
                pass_.has_page = True
            except APIError as e:
                if not _is_missing_column(e, 'page'):
                    raise
                pass_.has_page = False
                logger.info("list_items has no page column yet; syncing without it")
                rows, complete = self._paged(pass_, reader(ROW_COLUMNS))
        else:
            rows, complete = self._paged(pass_, reader(ROW_COLUMNS))
        for row in rows:
            rid = row.get('id')
            pass_.where[rid] = (row.get('list_id', cloud_list_id), row.get('note'), row.get('tags'))
            pass_.rows_by_id[rid] = row
        return rows, complete

    def _read_user_lists(self, pass_, cols):
        client = pass_.client
        return self._paged(pass_, lambda after: _after(client.table('user_lists').select(cols)
                                                       .eq('user_id', pass_.user_id), after))

    def _read_projects(self, pass_, cols):
        """Every cloud project of the account, in keyset pages as the lists are: (rows, complete)."""
        client = pass_.client
        return self._paged(pass_, lambda after: _after(client.table('projects').select(cols)
                                                       .eq('user_id', pass_.user_id), after))

    def _confirm(self, pass_, ids):
        """Where are these remembered rows now? Fills pass_.where, pass_.absent and pass_.locate_ok."""
        ask = sorted({i for i in ids if i is not None and i not in pass_.where}, key=_id_key)
        client = pass_.client

        def reader(chunk, cols):
            return lambda after: _after(client.table('list_items').select(cols).in_('id', chunk), after)

        for n in range(0, len(ask), CONFIRM_CHUNK):
            chunk = ask[n:n + CONFIRM_CHUNK]
            try:
                try:
                    cols = CONFIRM_COLUMNS + (', page' if pass_.has_page else '')
                    got, complete = self._paged(pass_, reader(chunk, cols))
                except APIError as e:
                    if not (pass_.has_page and _is_missing_column(e, 'page')):
                        raise
                    pass_.has_page = False
                    got, complete = self._paged(pass_, reader(chunk, CONFIRM_COLUMNS))
            except Exception as e:
                logger.info("Could not confirm where %d remembered row(s) are: %s", len(chunk), e)
                pass_.locate_ok = False
                continue
            for row in got:
                rid = row.get('id')
                if rid not in pass_.where:
                    pass_.where[rid] = (row.get('list_id'), row.get('note'), row.get('tags'))
                    pass_.rows_by_id[rid] = row
                    pass_.confirmed_only.add(rid)
            if complete:
                returned = {row.get('id') for row in got}
                pass_.absent.update(i for i in chunk if i not in returned)
            else:
                pass_.locate_ok = False

    def _may_tombstone(self, pass_, rec, cloud_list_id, read_complete, lists_complete, cloud_list_ids):
        """May this remembered row be taken as removed on the website?

        Only from a complete, authenticated reading: the row's own list read in full (or that list gone
        from a complete list of lists), and the row confirmed absent.
        """
        if cloud_list_id is not None and rec.get('list') == cloud_list_id:
            if not read_complete:
                return False
        elif not (lists_complete and rec.get('list') not in cloud_list_ids):
            return False
        return pass_.locate_ok and rec['id'] in pass_.absent and pass_.auth_ok

    @staticmethod
    def _tombstone(item_id, item, list_key, result):
        rec = item['cloud_rows'][list_key]
        rec['gone'] = True
        rec.pop('differs', None)
        result['web_removed'].append((item_id, list_key))
        logger.info("Entry %s in list %s was removed on the website; kept here, not uploaded again",
                    item_id, list_key)

    # ------------------------------------------------------------------
    # Items that share a row
    # ------------------------------------------------------------------

    def _repair_shared_cloud_rows(self, store, merge, has_page=False):
        """Items that hold the same cloud row: fold certain duplicates (download), split the rest.

        Returns the items that wait for a download to fold them (the upload skips them).
        """
        items = store.setdefault('items', {})
        holders = collections.defaultdict(set)
        for iid, it in list(items.items()):
            if it.get('cloud_id') is not None:
                holders[it['cloud_id']].add(iid)
            for _, rec in _records(it):
                if not rec.get('gone') and rec.get('id') is not None:
                    holders[rec['id']].add(iid)
        waiting = set()
        for rid in sorted(holders, key=_id_key):
            live = sorted((i for i in holders[rid] if i in items), key=str)
            if len(live) < 2:
                if live and items[live[0]].get('cloud_id') == rid:
                    items[live[0]].pop('cloud_id', None)  # a legacy id has no other use
                continue
            ident = {i: _item_identity(i, items[i]) for i in live}
            if all(_same_entry(ident[live[0]], ident[i], has_page) for i in live[1:]):
                keep = max(live, key=lambda i: (items[i].get('modified', items[i].get('added', 0)) or 0, str(i)))
                others = [i for i in live if i != keep]
                if not merge:
                    waiting.update(others)
                    continue
                kept = items[keep]
                for other in others:
                    it = items[other]
                    for lst in it.get('lists', []):
                        if lst not in kept.setdefault('lists', []):
                            kept['lists'].append(lst)
                    kept['note'] = _keep_both(kept.get('note'), it.get('note'))
                    kept['tags'] = _union(kept.get('tags'), it.get('tags'))
                    for key, rec in _records(it):
                        kept.setdefault('cloud_rows', {}).setdefault(key, rec)
                    del items[other]
                kept.pop('cloud_id', None)
                logger.info("Folded %d duplicate local item(s) sharing cloud row %s", len(others), rid)
            else:
                for i in live:
                    it = items[i]
                    if it.get('cloud_id') == rid:
                        it.pop('cloud_id', None)
                    for key, rec in list(_records(it)):
                        if rec.get('id') == rid and not rec.get('gone'):
                            _drop_record(it, key)
                logger.info("Split %d local items that shared cloud row %s", len(live), rid)
        return waiting

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    def sync_to_cloud(self, data=None, should_stop=None, progress=None, backfill_pages=True, withdrawn=None,
                      on_recorded=None) -> Dict[str, Any]:
        """
        Push local lists and items to Supabase.

        data: a copy of the store to upload from (the desktop's runner, which installs
        the copy's cloud identity itself: ListsManager.begin_upload/finish_upload); the
        live store otherwise, snapshotted first and saved after. should_stop() is asked
        before every request; progress(done, total) counts the lists whose entries are
        written (0 of total as the upload begins), and progress(0, 0, 'deletes') says the
        removals are being sent; backfill_pages=False sends no update whose only change is the page;
        withdrawn() names the memberships removed meanwhile, which are then neither
        inserted nor moved; on_recorded(report) hears of every record and cloud-id write.

        Returns:
            Dict with 'success', 'lists_pushed', 'items_pushed', 'items_failed',
            'notes_kept', 'notes_too_long', 'notes_differing', 'unchecked', 'waiting',
            'web_removed', 'lists_not_uploaded', 'rows_deleted', 'removals_failed',
            'deferred', 'stopped', 'complete', 'error'.
            notes_kept counts memberships (one per list whose row kept its differing
            note or tags); notes_too_long and notes_differing count entries (what the
            dialog shows). A removal that could not be sent counts in items_failed and
            removals_failed; a move waiting for its list (in the Trash) in deferred.
        """
        if not self.is_sync_available():
            return {'success': False, 'error': 'Sync not available'}

        if not self._sync_lock.acquire(blocking=False):
            return {'success': False, 'error': 'Sync already in progress'}
        try:
            # An upload changes nothing local except the cloud ids it records, so
            # it goes ahead without its snapshot.
            if data is None and not self._backup_local_data('upload'):
                logger.warning("Proceeding with sync despite backup failure")

            client = self._get_client()
            if not client:
                return {'success': False, 'error': 'No Supabase client'}
            pass_ = _Pass(client, self._user_id, should_stop=should_stop, withdrawn=withdrawn, report=on_recorded)
            if not backfill_pages:
                pass_.backfill_left = 0
            store = self.lists_manager.data if data is None else data
            return self._upload(store, client, pass_=pass_, on_progress=progress, save=data is None)
        finally:
            self._sync_lock.release()

    @staticmethod
    def _upload_result():
        return {'success': False, 'lists_pushed': 0, 'items_pushed': 0, 'items_failed': 0,
                'notes_kept': 0, 'notes_too_long': 0, 'notes_differing': 0, 'unchecked': 0,
                'waiting': 0, 'web_removed': [], 'lists_not_uploaded': [], 'rows_deleted': 0,
                'removals_failed': 0, 'deferred': 0, 'stopped': False, 'complete': False, 'error': None}

    def _upload(self, store, client, only_list=None, only_item=None, pass_=None, on_progress=None, save=True):
        if pass_ is None:
            pass_ = _Pass(client, self._user_id)
        session = _session_user(client)
        if session is not None and session != str(pass_.user_id):
            logger.warning("The session belongs to another user than the one lists sync was set up for")
            return {'success': False, 'error': 'Sync not available'}
        pass_.session_before = session
        self._guard(store, pass_)
        result = self._upload_result()
        lists = dict(store.get('lists') or {})
        # names of the lists an upload writes, in order: on an exception the one being
        # written and the ones after it are reported as not uploaded
        progress = {'lists': [lists[lid].get('name', 'Unnamed') for lid in _list_order(store)
                              if lid in lists and _syncable_list(lid, lists[lid])
                              and (only_list is None or lid == only_list)],
                    'writing': None, 'tell': on_progress, 'written': 0}
        # what the progress counts: the lists whose entries this upload writes (a list in the
        # Trash sends its own state only), each once; the deletes are a step of their own
        progress['total'] = sum(1 for lid in _list_order(store)
                                if lid in lists and _syncable_list(lid, lists[lid])
                                and not lists[lid].get('deleted_at') and (only_list is None or lid == only_list))
        if progress['total'] and only_item is None:
            self._tell_progress(progress, 0, progress['total'])
        try:
            if only_list is None:
                pushable, lists_complete, cloud_list_ids = self._push_projects_and_lists(pass_, store, result,
                                                                                         progress)
            else:
                ld = store['lists'][only_list]
                pushable = [(only_list, ld['cloud_id'])]
                lists_complete, cloud_list_ids = False, set()
            self._push_list_items(pass_, store, pushable, lists_complete, cloud_list_ids, result, progress,
                                  only_item=only_item)
            _drop_left_tombstones(store.get('items') or {})
            if save:
                self.lists_manager.save()
            self._last_sync = time.time()
            if result['items_failed']:
                result['success'] = False
                result['error'] = UPLOAD_PARTLY_FAILED.format(result['items_pushed'], result['items_failed'])
            else:
                result['success'] = True
        except _Stopped:
            # what it recorded so far stays (a copy's records are installed by the runner)
            logger.info("Upload stopped before its next request")
            result['stopped'] = True
            result['error'] = 'Sync stopped'
            self._not_uploaded(result, progress)
        except Exception as e:
            logger.error(f"Error syncing to cloud: {e}")
            result['error'] = str(e)
            self._not_uploaded(result, progress)
        result['notes_too_long'] = len(pass_.too_long)
        result['notes_differing'] = count_differing_notes(store)
        result['complete'] = bool(result['success'] and not result['unchecked'] and not result['waiting']
                                  and not result['lists_not_uploaded'])
        return result

    @staticmethod
    def _not_uploaded(result, progress):
        names = progress['lists']
        start = progress['writing'] if progress['writing'] is not None else 0
        for name in names[start:]:
            if name not in result['lists_not_uploaded']:
                result['lists_not_uploaded'].append(name)

    def _push_projects_and_lists(self, pass_, store, result, progress):
        """Projects and lists as before, except that a cloud list has at most one local owner."""
        client = pass_.client
        user_id = pass_.user_id
        # Existing cloud projects, read in full (to prevent duplicates)
        cloud_projects, projects_complete = self._read_projects(pass_, 'id, name')
        existing_cloud_projects = {proj['name']: proj['id'] for proj in cloud_projects}
        valid_project_ids = {proj['id'] for proj in cloud_projects}

        # Push projects first (so we have cloud IDs for list references)
        local_projects = store.get('projects', {})
        local_project_to_cloud = {}
        # Projects a read that did not reach the end left unmatched: kept as they are this pass
        # (no cloud id dropped, none created), and their lists' project is not sent
        projects_waiting = set()
        report = pass_.report

        for proj_id, proj_data in list(local_projects.items()):
            cloud_proj_id = proj_data.get('cloud_id')
            proj_name = proj_data.get('name', 'Unnamed')
            # Validate cloud_id still exists
            if cloud_proj_id and cloud_proj_id not in valid_project_ids:
                if not projects_complete:
                    # Not seen by a read that did not reach the end: it may still be there
                    logger.info("The cloud projects were not all read; project '%s' (cloud project %s) waits "
                                "for the next upload", proj_name, cloud_proj_id)
                    projects_waiting.add(proj_id)
                    continue
                logger.debug(f"Clearing stale cloud_id {cloud_proj_id} for project '{proj_data.get('name')}'")
                cloud_proj_id = None
                _set_field(store, 'projects', proj_id, 'cloud_id', None, report)
            if not cloud_proj_id and proj_name not in existing_cloud_projects and not projects_complete:
                # A cloud project of this name may lie past what the read returned: create none
                logger.info("The cloud projects were not all read; project '%s' is not created in the cloud "
                            "until the next upload", proj_name)
                projects_waiting.add(proj_id)
                continue

            proj_payload = {
                'user_id': user_id,
                'name': proj_name,
                'color': proj_data.get('color', '#4CAF50')
            }

            pass_.check()
            if cloud_proj_id:
                # Update existing cloud project
                client.table('projects').update(proj_payload).eq(
                    'id', cloud_proj_id
                ).execute()
                local_project_to_cloud[proj_id] = cloud_proj_id
            elif proj_name in existing_cloud_projects:
                # Project with same name exists - use it
                cloud_proj_id = existing_cloud_projects[proj_name]
                _set_field(store, 'projects', proj_id, 'cloud_id', cloud_proj_id, report)
                local_project_to_cloud[proj_id] = cloud_proj_id
                client.table('projects').update(proj_payload).eq(
                    'id', cloud_proj_id
                ).execute()
            else:
                # Create new cloud project
                response = client.table('projects').insert(proj_payload).execute()
                if response.data:
                    cloud_proj_id = response.data[0]['id']
                    _set_field(store, 'projects', proj_id, 'cloud_id', cloud_proj_id, report)
                    local_project_to_cloud[proj_id] = cloud_proj_id
                    existing_cloud_projects[proj_name] = cloud_proj_id

        # Existing cloud lists, read in full: every same-name list and every id
        cloud_lists, lists_complete = self._read_user_lists(pass_, 'id, name')
        cloud_list_ids = {lst['id'] for lst in cloud_lists}
        by_name = collections.defaultdict(list)
        for lst in cloud_lists:
            by_name[lst.get('name')].append(lst['id'])
        logger.debug(f"Found {len(cloud_lists)} existing cloud lists")

        local_lists = dict(store.get('lists', {}))   # the lists as the pass began (their dicts are live)
        order = [lid for lid in _list_order(store) if lid in local_lists and _syncable_list(lid, local_lists[lid])]
        # One local owner per cloud list: the first in the list order keeps a shared id.
        held = set()
        for list_id in order:
            cloud_id = local_lists[list_id].get('cloud_id')
            if cloud_id is None or cloud_id not in cloud_list_ids:
                continue
            if cloud_id in held:
                logger.info("Two local lists held cloud list %s; %s gets its own", cloud_id, list_id)
                _set_field(store, 'lists', list_id, 'cloud_id', None, report)
            else:
                held.add(cloud_id)

        pushable = []
        for list_id in order:
            list_data = local_lists[list_id]
            cloud_id = list_data.get('cloud_id')
            list_name = list_data.get('name', 'Unnamed')
            if cloud_id and cloud_id not in cloud_list_ids:
                if not lists_complete:
                    # Not seen by a read that did not reach the end: the cloud list may still be
                    # there. Keep the id and leave the list for a pass that reads every list.
                    logger.info("The cloud lists were not all read; list '%s' (cloud list %s) waits for the "
                                "next upload", list_name, cloud_id)
                    result['lists_not_uploaded'].append(list_name)
                    continue
                # Gone from a complete read: deleted in the cloud
                logger.debug(f"Clearing stale cloud_id {cloud_id} for list '{list_name}'")
                cloud_id = None
                _set_field(store, 'lists', list_id, 'cloud_id', None, report)

            # Map local project_id to cloud project_id
            local_proj_id = list_data.get('project_id')
            cloud_proj_id = local_project_to_cloud.get(local_proj_id) if local_proj_id else None
            project_waits = local_proj_id in projects_waiting

            # Handle soft delete - convert local timestamp to ISO format for cloud
            local_deleted_at = list_data.get('deleted_at')
            cloud_deleted_at = None
            if local_deleted_at:
                from datetime import datetime, timezone
                cloud_deleted_at = datetime.fromtimestamp(local_deleted_at, tz=timezone.utc).isoformat()

            list_payload = {
                'user_id': user_id,
                'name': list_name,
                'name_en': list_data.get('name_en', list_name),
                'color': list_data.get('color', '#FFD700'),
                'is_default': list_data.get('is_default', False),
                'is_system': False,
                'project_id': cloud_proj_id,
                'deleted_at': cloud_deleted_at
            }
            # An existing cloud list gets this list's name only when it was renamed here
            # (LIST_NAME_UNSENT): another name there was given on the website (or by
            # another computer), and this list takes it at its next Download.
            update_payload = dict(list_payload)
            if not list_data.get(LIST_NAME_UNSENT):
                del update_payload['name'], update_payload['name_en']
            if project_waits:
                # its project could not be matched this pass: the cloud list keeps the project it has
                del update_payload['project_id']

            pass_.check()
            if cloud_id:
                # Update existing cloud list by stored cloud_id
                sent = update_payload
                response = client.table('user_lists').update(update_payload).eq(
                    'id', cloud_id
                ).execute()
            else:
                free = sorted((cid for cid in by_name.get(list_name, ()) if cid not in held), key=_id_key)
                if not free and not lists_complete:
                    # A cloud list of this name may lie past what the read returned: create none
                    # this pass. (A free one that was read is safe to take: a read ends short only
                    # after returning every list below the last id it reached.)
                    logger.info("The cloud lists were not all read; list '%s' is not created in the cloud "
                                "until the next upload", list_name)
                    result['lists_not_uploaded'].append(list_name)
                    continue
                if free:
                    # The lowest same-name cloud list no local list holds (the download picks the same)
                    cloud_id = free[0]
                    _set_field(store, 'lists', list_id, 'cloud_id', cloud_id, report)
                    held.add(cloud_id)
                    logger.debug(f"Found existing cloud list '{list_name}' with ID {cloud_id}")
                    sent = update_payload
                    response = client.table('user_lists').update(update_payload).eq(
                        'id', cloud_id
                    ).execute()
                else:
                    # Create new cloud list (no free one of that name exists)
                    sent = list_payload
                    response = client.table('user_lists').insert(list_payload).execute()
                    if response.data:
                        cloud_id = response.data[0]['id']
                        _set_field(store, 'lists', list_id, 'cloud_id', cloud_id, report)
                        held.add(cloud_id)

            if cloud_id and response.data:
                # the row answered, so the list's state is in the cloud (a write the session
                # could not see changed nothing, and the list's own state still stands) --
                # unless its project waits for the next upload
                if not project_waits:
                    _set_field(store, 'lists', list_id, LIST_STATE_UNSENT, None, report)
                if 'name' in sent and list_data.get('name') == sent['name']:
                    _set_field(store, 'lists', list_id, LIST_NAME_UNSENT, None, report)   # its rename is there too
            if project_waits and list_name not in result['lists_not_uploaded']:
                result['lists_not_uploaded'].append(list_name)     # its project has not reached the cloud
            result['lists_pushed'] += 1

            # Skip syncing items for deleted lists
            if local_deleted_at:
                logger.debug(f"Skipping items sync for deleted list '{list_name}'")
                continue
            if not cloud_id:
                result['lists_not_uploaded'].append(list_name)
                continue
            pushable.append((list_id, cloud_id))
        return pushable, lists_complete, cloud_list_ids

    def _push_list_items(self, pass_, store, pushable, lists_complete, cloud_list_ids, result, progress,
                         only_item=None):
        """The item half of an upload, for the given (local list, cloud list) pairs.

        Phase 1 reads every list and pairs rows with memberships, then confirms where
        the remembered rows it did not see are; Phase 2 writes, list by list; Phase 3
        sends the deletes (a whole upload only). My Library (LOCAL) entries are
        skipped: no insert, update, move or record.
        """
        items = store.setdefault('items', {})
        waiting = self._repair_shared_cloud_rows(store, merge=False, has_page=False)
        orphan_ids = _orphan_ids(items)
        # A row waiting for its delete is paired with no membership in the cloud list its
        # removal names (putting the entry back in that list gave it its row back already);
        # found in another list, another computer moved it there, and it is that list's to pair.
        pending_in = collections.defaultdict(set)
        for entry in self._pending(pass_, store):
            pending_in[entry.get('list')].add(entry['id'])
        lists = store.get('lists', {})
        plans = []
        for list_id, cloud_id in pushable:
            members = []
            for iid, it in list(items.items()):
                if only_item is not None and iid != only_item:
                    continue
                if list_id not in list(it.get('lists') or []):
                    continue
                if is_local_sys_id(it.get('sys_id', iid)):
                    continue
                if iid in waiting:
                    result['waiting'] += 1
                    continue
                members.append((iid, it))
            rows, complete = self._fetch_list_rows(pass_, cloud_id)
            matched = _match_rows(rows, members, list_id, bool(pass_.has_page), pass_.claimed,
                                  orphan_ids | pending_in.get(cloud_id, set()), set())
            member_of = dict(members)
            bases = {rid: _base_of(member_of[iid], rid) for rid, iid in matched.items()}
            plans.append({'list': list_id, 'cloud': cloud_id, 'members': members, 'complete': complete,
                          'matched': matched, 'bases': bases})

        ask = set()
        for plan in plans:
            done = set(plan['matched'].values())
            for iid, it in plan['members']:
                if iid in done:
                    continue
                rec = _live_record(it, plan['list'])
                if rec:
                    ask.add(rec['id'])
                ask.update(r['id'] for _, r in _orphans(it))
        deletes = only_item is None and self._has_deletes(pass_, store)
        if deletes:
            # Phase 3 needs to know where every pending removal and every moved row is
            ask.update(e['id'] for e in self._pending(pass_, store))
            for it in list(items.values()):
                ask.update(r['id'] for _, r in _orphans(it))
        self._confirm(pass_, ask)
        pass_.prove_auth()
        owners = {ld.get('cloud_id'): lid for lid, ld in list(lists.items()) if ld.get('cloud_id') is not None}
        self._hold_conflicts(pass_, plans, owners)

        names = [lists.get(p['list'], {}).get('name', 'Unnamed') for p in plans]
        for n, plan in enumerate(plans):
            if only_item is None:
                progress['writing'] = progress['lists'].index(names[n]) if names[n] in progress['lists'] else None
            failed_before = result['items_failed']
            self._write_list(pass_, store, plan, lists_complete, cloud_list_ids, owners, result)
            if result['items_failed'] > failed_before and names[n] not in result['lists_not_uploaded']:
                result['lists_not_uploaded'].append(names[n])
            if only_item is None:
                progress['written'] = progress.get('written', 0) + 1
                total = progress.get('total') or len(plans)
                self._tell_progress(progress, min(progress['written'], total), total)
        progress['writing'] = None
        if deletes:
            if only_item is None:
                self._tell_progress(progress, 0, 0, 'deletes')    # a step of its own, not a list
            read = {plan['cloud']: plan['complete'] for plan in plans}
            self._send_deletes(pass_, store, result, read, lists_complete, cloud_list_ids)

    @staticmethod
    def _tell_progress(progress, done, total, what=None):
        """progress['tell'](done, total), or (done, total, what) for a step that is not a list."""
        tell = progress.get('tell')
        if tell is None:
            return
        try:
            if what is None:
                tell(done, total)
            else:
                tell(done, total, what)
        except Exception as e:
            logger.debug("Upload progress callback failed: %s", e)

    def _hold_conflicts(self, pass_, plans, owners):
        """Entries whose note (or tags) differ on one of their rows and may not be replaced there.

        Their note is then written to none of their rows in this pass: replacing it in
        the other lists would spread one side of an unresolved difference, and another
        computer would meet the two sides on different rows.
        """
        for plan in plans:
            for iid, it in plan['members']:
                rows = []
                rid = next((r for r, i in plan['matched'].items() if i == iid), None)
                raw = (it.get('cloud_rows') or {}).get(plan['list'])
                if rid is not None:
                    rows.append((rid, plan['bases'].get(rid)))
                elif isinstance(raw, dict) and raw.get('gone'):
                    pass                # removed on the website: nothing is written for it
                else:
                    rec = _live_record(it, plan['list'])
                    if rec is not None and pass_.where.get(rec['id'], (None,))[0] == plan['cloud']:
                        rows.append((rec['id'], rec))
                    elif rec is None:   # the orphan's row it will move or adopt
                        choice = self._orphan_choice(pass_, it, plan['list'], plan['cloud'], owners)
                        if choice is not None:
                            rows.append((choice[2]['id'], choice[2]))
                for rid, base in rows:
                    loc = pass_.where.get(rid)
                    if loc is None:
                        continue
                    b_note = base.get('note') if base else None
                    b_tags = base.get('tags') if base else None
                    if not _same_text(it.get('note'), loc[1]) and not (b_note is not None
                                                                      and _same_text(loc[1], b_note)):
                        pass_.held_notes.add(iid)
                    if _tagset(it.get('tags')) != _tagset(loc[2]) and not (
                            b_tags is not None and _tagset(loc[2]) == _tagset(b_tags)):
                        pass_.held_tags.add(iid)

    def _write_list(self, pass_, store, plan, lists_complete, cloud_list_ids, owners, result):
        list_id, cloud_id = plan['list'], plan['cloud']
        row_of = {iid: rid for rid, iid in plan['matched'].items()}
        inserts = []
        for iid, it in plan['members']:
            rid = row_of.get(iid)
            if rid is not None:
                row = pass_.rows_by_id.get(rid) or {}
                self._write_matched(pass_, store, iid, it, list_id, cloud_id, rid, row, row.get('note'),
                                    row.get('tags'), plan['bases'].get(rid), result)
                continue
            self._write_unmatched(pass_, store, iid, it, plan, lists_complete, cloud_list_ids, owners,
                                  result, inserts)
        if inserts:
            self._insert_rows(pass_, store, list_id, cloud_id, inserts, result)

    def _write_unmatched(self, pass_, store, iid, it, plan, lists_complete, cloud_list_ids, owners, result,
                         inserts):
        """A membership that no row of its list's read paired with."""
        list_id, cloud_id = plan['list'], plan['cloud']
        recs = it.get('cloud_rows') or {}
        rec = recs.get(list_id) if isinstance(recs.get(list_id), dict) else None
        if rec is not None and rec.get('gone'):
            return                                               # removed on the website: not uploaded again
        if rec is not None and rec.get('id') is not None:
            rid = rec['id']
            loc = pass_.where.get(rid)
            if loc is not None and loc[0] == cloud_id and rid not in pass_.claimed:
                pass_.claimed.add(rid)                           # the row is there after all
                self._write_matched(pass_, store, iid, it, list_id, cloud_id, rid, pass_.rows_by_id.get(rid),
                                    loc[1], loc[2], dict(rec), result)
                return
            if loc is not None:
                _drop_record(it, list_id)                        # stale: the row is in another list
                rec = None
            elif rec.get('list') != cloud_id and lists_complete and rec.get('list') in cloud_list_ids:
                _drop_record(it, list_id)                        # stale: its list is no longer this list's own
                rec = None
            elif rid in pass_.absent:                            # gone from the website?
                if self._may_tombstone(pass_, rec, cloud_id, plan['complete'], lists_complete, cloud_list_ids):
                    self._tombstone(iid, it, list_id, result)
                else:
                    result['unchecked'] += 1
                return
            else:
                result['unchecked'] += 1                         # neither located nor absent: wait
                return
        elif rec is not None:
            _drop_record(it, list_id)
        if self._move_orphan(pass_, store, iid, it, plan, owners, result, inserts):
            return
        if plan['complete']:                                     # nothing remembered: insert
            inserts.append((iid, it))
        else:
            result['unchecked'] += 1

    @staticmethod
    def _orphan_choice(pass_, it, list_id, cloud_id, owners, drop_absent=False):
        """The orphan whose row this membership takes (moved or adopted): (rank, key, record), or None.

        Only a row bound for this list ('to'), or one bound for no list the item is in; a
        row bound for another of its lists waits for that one. In this list's own cloud
        list first (adopted), then located in another list, then not located; ties by the
        lowest row id. An unbound orphan whose row is in the list of another list this
        item is in is that membership's to adopt.
        """
        lists = it.get('lists') or []
        cands = []
        for key, rec in _orphans(it):
            rid = rec['id']
            if rid in pass_.claimed:
                continue
            to = rec.get('to')
            if to is not None and to != list_id and to in lists:
                continue                                         # it waits for its own destination
            loc = pass_.where.get(rid)
            if loc is None and pass_.locate_ok and pass_.auth_ok and rid in pass_.absent:
                if drop_absent:
                    _drop_record(it, key)                        # its row is gone: nothing to move
                continue
            where_now = loc[0] if loc is not None else rec.get('list')
            owner = owners.get(where_now)
            if to != list_id and owner is not None and owner != list_id and owner in lists:
                continue                                         # that list of this item will adopt it
            rank = 0 if (loc is not None and loc[0] == cloud_id) else (1 if loc is not None else 2)
            cands.append((rank, _id_key(rid), key, rec))
        if not cands:
            return None
        rank, _, key, rec = min(cands)
        return rank, key, rec

    def _move_orphan(self, pass_, store, iid, it, plan, owners, result, inserts):
        """Move (or adopt) a row this item left in a list it is no longer in."""
        list_id, cloud_id = plan['list'], plan['cloud']
        choice = self._orphan_choice(pass_, it, list_id, cloud_id, owners, drop_absent=True)
        if choice is None:
            return False
        rank, key, rec = choice
        rid = rec['id']
        pass_.claimed.add(rid)
        loc = pass_.where.get(rid)
        if loc is None and rec.get('list') == cloud_id:
            # recorded in this list's own cloud list but not located in this pass: nothing
            # to move it from; it is adopted once a read finds it there
            result['unchecked'] += 1
            return True
        if rank == 0:                                            # adopt: the row is already in this list
            base = dict(rec)
            _drop_record(it, key)
            _remember(pass_, store, iid, it, list_id, rid, cloud_id, base.get('note'), base.get('tags'),
                      base.get('differs', False))
            self._write_matched(pass_, store, iid, it, list_id, cloud_id, rid, pass_.rows_by_id.get(rid),
                                loc[1], loc[2], base, result)
            return True
        if (iid, list_id) in pass_.withdrawn_now():
            return True                                          # removed from this list meanwhile: not moved
        src = loc[0] if loc is not None else rec.get('list')
        row = pass_.rows_by_id.get(rid) if loc is not None else None
        base = dict(rec)
        change, note_c, tags_c, kept, bases = self._changes(pass_, iid, it, row, loc, base)
        status, sent = self._patch(pass_, rid, src, dict(change, list_id=cloud_id), note_c, tags_c)
        if status == 'too_long':
            pass_.too_long.add(iid)
            bases = self._agreed_bases(it, loc[1], loc[2], base)
            if (iid, list_id) in pass_.withdrawn_now():
                return True
            status = self._send_rest(pass_, rid, src, sent)
        elif status == 'nomatch':
            now = self._reread(pass_, rid)
            if now is None:
                if _session_user(pass_.client) == str(pass_.user_id):
                    return self._orphan_gone(it, key, plan, result, inserts, iid)   # its row is gone
                status = 'error'
            elif now.get('list_id') == src and ('note' in sent or 'tags' in sent):
                kept = True
                logger.info("Row %s changed on the website while it was being moved; moved it, note kept", rid)
                bases = self._agreed_bases(it, loc[1], loc[2], base)
                if (iid, list_id) in pass_.withdrawn_now():
                    return True
                status = self._send_rest(pass_, rid, src, sent)
            else:
                status = 'error'
        if status in ('ok', 'unchanged'):
            if kept:
                result['notes_kept'] += 1
            _drop_record(it, key)
            _remember(pass_, store, iid, it, list_id, rid, cloud_id, bases[0], bases[1], kept)
            result['items_pushed'] += 1
        else:
            result['items_failed'] += 1                          # the orphan stays for the next pass
        return True

    def _orphan_gone(self, it, key, plan, result, inserts, iid):
        _drop_record(it, key)
        if plan['complete']:
            inserts.append((iid, it))
        else:
            result['unchecked'] += 1
        return True

    def _reread(self, pass_, rid):
        pass_.check()
        try:
            resp = pass_.client.table('list_items').select(CONFIRM_COLUMNS).eq('id', rid).execute()
        except Exception as e:
            logger.info("Could not re-read row %s: %s", rid, e)
            return {'list_id': UNKNOWN_LIST}
        rows = resp.data or []
        return rows[0] if rows else None

    def _changes(self, pass_, iid, it, row, loc, base_rec):
        """What an upload may change on a paired row.

        loc: (cloud list, note, tags) as read, or None when the row's values are not
        known (then no note or tag is written). row: the full row as read, or None
        (then no identity field is written). Returns (changed fields, note filter,
        tags filter, kept, (note base, tags base) once the change has landed).
        """
        x_note = it.get('note') or ''
        x_tags = list(it.get('tags') or [])
        change = {}
        note_c = tags_c = None
        kept = False
        new_note = base_rec.get('note') if base_rec else None
        new_tags = base_rec.get('tags') if base_rec else None
        if loc is not None:
            c_note_raw, c_tags_raw = loc[1], loc[2]
            c_note, c_tags = c_note_raw or '', list(c_tags_raw or [])
            if _same_text(x_note, c_note):
                new_note = c_note
            elif new_note is not None and _same_text(c_note, new_note) and iid not in pass_.held_notes:
                change['note'] = x_note           # only this computer changed it
                note_c = c_note_raw
                new_note = x_note
            else:
                kept = True
            if _tagset(x_tags) == _tagset(c_tags):
                new_tags = c_tags
            elif new_tags is not None and _tagset(c_tags) == _tagset(new_tags) and iid not in pass_.held_tags:
                change['tags'] = x_tags
                tags_c = c_tags_raw
                new_tags = x_tags
            else:
                kept = True
        if row:
            has_page = bool(pass_.has_page)
            me = _item_identity(iid, it)
            theirs = _row_identity(row, has_page)
            # identity fields only fill an empty value, and only between certainly one entry
            if _same_entry(me, theirs, has_page):
                if me[1] and not theirs[1]:
                    change['fl_id'] = me[1]
                if has_page and me[2] and not theirs[2]:
                    change['page'] = me[2]
            override = it.get('shelfmark_override')
            if override and override != row.get('shelfmark'):
                change['shelfmark'] = override
        if set(change) == {'page'}:
            if pass_.backfill_left > 0:
                pass_.backfill_left -= 1
            else:
                change = {}
        return change, note_c, tags_c, kept, (new_note, new_tags)

    def _write_matched(self, pass_, store, iid, it, list_id, cloud_id, rid, row, c_note, c_tags, base_rec, result):
        """A row paired with this membership; only what changed, conditional on what was read."""
        change, note_c, tags_c, kept, bases = self._changes(pass_, iid, it, row, (cloud_id, c_note, c_tags),
                                                            base_rec)
        status = 'ok'
        if change:
            status, sent = self._patch(pass_, rid, cloud_id, change, note_c, tags_c)
            if status == 'too_long':
                pass_.too_long.add(iid)
                bases = self._agreed_bases(it, c_note, c_tags, base_rec)
                status = self._send_rest(pass_, rid, cloud_id, sent)
            elif status == 'nomatch':
                now = self._reread(pass_, rid)
                if now is None or now.get('list_id') != cloud_id:
                    status = 'error'
                else:
                    if 'note' in sent or 'tags' in sent:
                        kept = True
                        logger.info("Row %s changed on the website since it was read; its note was kept", rid)
                    bases = self._agreed_bases(it, c_note, c_tags, base_rec)
                    status = self._send_rest(pass_, rid, cloud_id, sent)
        if status in ('ok', 'unchanged'):
            if kept:
                result['notes_kept'] += 1
            _remember(pass_, store, iid, it, list_id, rid, cloud_id, bases[0], bases[1], kept)
            result['items_pushed'] += 1
        else:
            result['items_failed'] += 1

    def _send_rest(self, pass_, rid, list_filter, sent):
        """After a note/tag change could not be made: send the row's other changed fields alone."""
        rest = {k: v for k, v in sent.items() if k not in ('note', 'tags')}
        if not rest:
            return 'ok'
        status, _ = self._patch(pass_, rid, list_filter, rest, None, None)
        return status

    @staticmethod
    def _agreed_bases(it, c_note, c_tags, base_rec):
        """Bases when a note/tag write did not happen: equal values are agreed, the rest unchanged."""
        b_note = base_rec.get('note') if base_rec else None
        b_tags = base_rec.get('tags') if base_rec else None
        if _same_text(it.get('note'), c_note):
            b_note = c_note or ''
        if _tagset(it.get('tags')) == _tagset(c_tags):
            b_tags = list(c_tags or [])
        return b_note, b_tags

    def _patch(self, pass_, rid, list_filter, payload, note_c, tags_c):
        """One PATCH of a row, filtered by id, list and the note/tags it replaces.

        Returns (status, payload sent): 'ok', 'nomatch', 'too_long' (the filter did not
        fit the gateway's URL limit: nothing written), 'unchanged' (a page-only change
        with the page column missing: nothing to send) or 'error'.
        """
        client = pass_.client

        def build(p):
            q = client.table('list_items').update(p).eq('id', rid).eq('list_id', list_filter)
            if 'note' in p:
                q = q.is_('note', 'null') if note_c is None else q.eq('note', note_c)
            if 'tags' in p:
                if tags_c is None:
                    q = q.is_('tags', 'null')
                else:
                    lit = _tags_filter_literal(tags_c)
                    q = q.contains('tags', lit).contained_by('tags', lit)
            return q

        if not pass_.has_page and 'page' in payload:
            payload = {k: v for k, v in payload.items() if k != 'page'}
            if not payload:
                return 'unchanged', payload
        for attempt in (0, 1):
            pass_.check()
            try:
                resp = build(payload).execute()
            except APIError as e:
                if attempt == 0 and 'page' in payload and _is_missing_column(e, 'page'):
                    pass_.has_page = False
                    logger.info("list_items has no page column in the API's schema yet; writing without it")
                    payload = {k: v for k, v in payload.items() if k != 'page'}
                    if not payload:
                        return 'unchanged', payload
                    continue
                if _is_url_too_long(e) and ('note' in payload or 'tags' in payload):
                    logger.info("Row %s: the cloud note or tags are too long to compare in a request", rid)
                    return 'too_long', payload
                logger.warning(f"list_items update failed for id={rid}: {e}")
                return 'error', payload
            except Exception as e:
                logger.warning(f"list_items update failed for id={rid}: {e}")
                return 'error', payload
            return ('ok' if resp.data else 'nomatch'), payload
        return 'error', payload

    def _insert_rows(self, pass_, store, list_id, cloud_id, members, result):
        """One batch per list; returned rows are recorded by identity, never by position."""
        client = pass_.client

        def payload_of(iid, it):
            sys_id, fl_id, page = _item_identity(iid, it)
            p = {'list_id': cloud_id, 'sys_id': sys_id, 'shelfmark': it.get('shelfmark_override') or None,
                 'title': None, 'fl_id': fl_id, 'note': it.get('note', '') or '',
                 'tags': list(it.get('tags', []) or [])}
            if pass_.has_page and page is not None:
                p['page'] = page
            return p

        def record(rows, batch):
            has_page = bool(pass_.has_page)
            waiting = collections.defaultdict(list)
            for p, iid, it in batch:
                waiting[_row_identity(p, has_page)].append((p, iid, it))
            for row in sorted(rows, key=lambda r: _id_key(r.get('id'))):
                result['items_pushed'] += 1
                if row.get('sys_id') is None or row.get('id') is None:
                    continue    # counted; the next pass pairs it by its content
                group = waiting.get(_row_identity(row, has_page))
                if group:
                    p, iid, it = group.pop(0)
                    _remember(pass_, store, iid, it, list_id, row['id'], cloud_id, p['note'], p['tags'])
            result['items_failed'] += max(0, len(batch) - len(rows))

        def attempt(batch):
            pass_.check()
            payloads = [p for p, _, _ in batch]
            return client.table('list_items').insert(payloads if len(payloads) > 1 else payloads[0]).execute()

        def left(batch):
            # read right before each insert request: an entry the user removed from this
            # list since the upload began is not inserted (nor counted)
            gone = pass_.withdrawn_now()
            return [b for b in batch if (b[1], list_id) not in gone] if gone else batch

        batch = left([(payload_of(iid, it), iid, it) for iid, it in members])
        if not batch:
            return
        try:
            try:
                resp = attempt(batch) if len(batch) > 1 else self._insert_one(pass_, batch[0])
            except APIError as e:
                if not _is_missing_column(e, 'page') or not any('page' in p for p, _, _ in batch):
                    raise
                pass_.has_page = False
                logger.info("list_items has no page column in the API's schema yet; inserting without it")
                batch = left([({k: v for k, v in p.items() if k != 'page'}, iid, it) for p, iid, it in batch])
                if not batch:
                    return
                resp = attempt(batch)
            record(resp.data or [], batch)
            return
        except Exception as e:
            if len(batch) == 1 or not _insert_rolled_back(e):
                logger.warning(f"list_items insert failed ({len(batch)} row(s)); not retried in this pass: {e}")
                result['items_failed'] += len(batch)
                return
            logger.warning(f"Batch insert failed, inserting one by one: {e}")
        for p, iid, it in batch:
            if (iid, list_id) in pass_.withdrawn_now():
                continue
            try:
                resp = self._insert_one(pass_, (p, iid, it))
            except Exception as item_err:
                logger.warning(f"list_items insert failed for sys_id={p.get('sys_id')}: {item_err}")
                result['items_failed'] += 1
                continue
            record(resp.data or [], [(p, iid, it)])

    def _insert_one(self, pass_, entry):
        p = entry[0]
        pass_.check()
        try:
            return pass_.client.table('list_items').insert(p).execute()
        except APIError as e:
            if 'page' not in p or not _is_missing_column(e, 'page'):
                raise
            pass_.has_page = False
            del p['page']
            pass_.check()
            return pass_.client.table('list_items').insert(p).execute()

    # ------------------------------------------------------------------
    # Upload, Phase 3: the deletes
    # ------------------------------------------------------------------

    @staticmethod
    def _pending(pass_, store):
        """This account's pending removals (another account's wait for it)."""
        return [e for e in list((store.get('cloud_deletes') or {}).values())
                if isinstance(e, dict) and e.get('account') == pass_.user_id and e.get('id') is not None]

    def _suppressed(self, pass_, store):
        """{cloud list id: the rows there this account waits to delete} (a download leaves them out)."""
        out = collections.defaultdict(set)
        for entry in self._pending(pass_, store):
            out[entry.get('list')].add(entry['id'])
        return out

    def _has_deletes(self, pass_, store):
        return bool(self._pending(pass_, store)) or any(
            _orphans(it) for it in list((store.get('items') or {}).values()))

    def _send_deletes(self, pass_, store, result, read, lists_complete, cloud_list_ids):
        """Explicit removals, then the moved rows whose destination has its own row.

        read: {cloud list id: whether this pass read it in full}, for the lists it read.
        """
        for entry in sorted(self._pending(pass_, store), key=lambda e: _id_key(e['id'])):
            self._send_explicit(pass_, store, entry, result, read, lists_complete, cloud_list_ids)
        for iid, it in list((store.get('items') or {}).items()):
            for key, rec in list(_orphans(it)):
                self._settle_moved_row(pass_, store, iid, it, key, rec, result)
        pending = store.get('cloud_deletes')
        if pending is not None and not pending:
            store.pop('cloud_deletes', None)

    @staticmethod
    def _unqueue(store, rid):
        pending = store.get('cloud_deletes') or {}
        pending.pop(str(rid), None)

    def _send_explicit(self, pass_, store, entry, result, read, lists_complete, cloud_list_ids):
        """One explicit removal: DELETE by id and the list this computer last knew, no other condition.

        A row found in another list was moved there by another computer: its move wins and
        nothing is deleted. A delete that fails, or matches nothing while the row cannot be
        placed, stays for the next upload and fails this one.
        """
        rid, lst = entry['id'], entry.get('list')
        loc = pass_.where.get(rid)
        if loc is not None and loc[0] != lst:
            logger.info("Row %s of a removed entry is now in another list; not deleted there", rid)
            self._unqueue(store, rid)
            return
        if (loc is None and pass_.locate_ok and pass_.auth_ok and rid in pass_.absent
                and (read.get(lst) or (lists_complete and lst not in cloud_list_ids))):
            # gone already: its list read in full without it (or that list gone), and not found by id
            self._unqueue(store, rid)
            return
        status = self._delete_row(pass_, rid, lst)
        if status == 'ok':
            self._unqueue(store, rid)
            result['rows_deleted'] += 1
            return
        if status == 'nomatch':
            now = self._reread(pass_, rid)
            if now is None:
                if _session_user(pass_.client) == str(pass_.user_id):
                    self._unqueue(store, rid)    # gone meanwhile
                    return
            elif now.get('list_id') is not UNKNOWN_LIST and now.get('list_id') != lst:
                logger.info("Row %s of a removed entry was moved to another list meanwhile; not deleted", rid)
                self._unqueue(store, rid)
                return
        logger.warning("The removal of row %s could not be sent; it is tried again at the next upload", rid)
        result['items_failed'] += 1
        result['removals_failed'] += 1

    def _settle_moved_row(self, pass_, store, iid, it, key, rec, result):
        """A moved row no membership took in Phase 2: gone, redundant, or waiting for its list.

        Redundant: its destination has its own row this pass. It is deleted where it was
        found, only as it was read and only while the entry holds its note and tags;
        otherwise it stays (marked differing) until a Download folds its text in.
        """
        rid = rec['id']
        loc = pass_.where.get(rid)
        if loc is None and pass_.locate_ok and pass_.auth_ok and rid in pass_.absent:
            _drop_record(it, key)                # its row is gone: nothing to move or delete
            return
        lists = list(it.get('lists') or [])
        to = rec.get('to')
        if to is not None and to in lists:
            ready = (iid, to) in pass_.recorded
        else:                                    # no destination: redundant once every list has its row
            mine = [lid for lid in lists
                    if _syncable_list(lid, (store.get('lists') or {}).get(lid) or {'is_system': True})]
            ready = bool(mine) and all((iid, lid) in pass_.recorded for lid in mine)
        if not ready or loc is None:
            result['deferred'] += 1              # its list is in the Trash, has no cloud list yet, or is unread
            return
        c_note, c_tags = loc[1], loc[2]
        x_note, x_tags = it.get('note') or '', list(it.get('tags') or [])
        b_note, b_tags = rec.get('note'), rec.get('tags')
        held = (((b_note is not None and _same_text(c_note, b_note)) or _holds(x_note, c_note or ''))
                and ((b_tags is not None and _tagset(c_tags) == _tagset(b_tags))
                     or _tagset(c_tags) <= _tagset(x_tags)))
        if held:
            status = self._delete_row(pass_, rid, loc[0], (c_note, c_tags))
            if status == 'ok':
                _drop_record(it, key)
                result['rows_deleted'] += 1
                return
            if status == 'too_long':
                pass_.too_long.add(iid)
                return
            if status == 'error':
                result['items_failed'] += 1
                return
        # the website changed the row: its text reaches the entry at the next Download
        result['notes_kept'] += 1
        rec['differs'] = True

    def _delete_row(self, pass_, rid, list_filter, cond=None):
        """One DELETE of a row, by id and list, and with cond=(note, tags) only as it was read.

        Returns 'ok' (the row was deleted), 'nomatch', 'too_long' (the condition did not fit
        the gateway's URL limit: nothing deleted) or 'error'.
        """
        q = pass_.client.table('list_items').delete().eq('id', rid).eq('list_id', list_filter)
        if cond is not None:
            note_c, tags_c = cond
            q = q.is_('note', 'null') if note_c is None else q.eq('note', note_c)
            if tags_c is None:
                q = q.is_('tags', 'null')
            else:
                lit = _tags_filter_literal(tags_c)
                q = q.contains('tags', lit).contained_by('tags', lit)
        pass_.check()
        try:
            resp = q.execute()
        except APIError as e:
            if cond is not None and _is_url_too_long(e):
                logger.info("Row %s: its note or tags are too long to compare in a request", rid)
                return 'too_long'
            logger.warning("list_items delete failed for id=%s: %s", rid, e)
            return 'error'
        except Exception as e:
            logger.warning("list_items delete failed for id=%s: %s", rid, e)
            return 'error'
        return 'ok' if resp.data else 'nomatch'

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def sync_from_cloud(self, should_stop=None) -> Dict[str, Any]:
        """
        Pull lists and items from Supabase and merge with local data: the fetch and the
        apply below, one after the other on this thread, under one hold of the lock.

        IMPORTANT: This only ADDS data from cloud, never removes local data.
        If cloud is empty, local data is preserved unchanged.

        Returns:
            Dict with 'success', 'lists_added', 'lists_updated', 'items_added',
            'notes_merged', 'tags_merged', 'notes_differing', 'unchecked', 'web_removed', 'error'.
            notes_merged, tags_merged and notes_differing count entries: notes_merged
            those whose note now holds a text kept under a marker line, tags_merged
            those whose tags were combined.
        """
        if not self.is_sync_available():
            return {'success': False, 'error': 'Sync not available'}

        if not self._sync_lock.acquire(blocking=False):
            return {'success': False, 'error': 'Sync already in progress'}
        try:
            state = self._fetch(remembered_ids(self.lists_manager.data, self._user_id), should_stop)
            if not state.get('success'):
                return self._fetch_failed(state)
            return self._apply(state)
        finally:
            self._sync_lock.release()

    @staticmethod
    def _download_result():
        return {'success': False, 'lists_added': 0, 'lists_updated': 0, 'items_added': 0, 'notes_merged': 0,
                'tags_merged': 0, 'notes_differing': 0, 'unchecked': 0, 'web_removed': [], 'error': None}

    def _fetch_failed(self, state):
        """A download whose fetch did not complete: nothing local was read or changed."""
        if state.get('error') == 'Sync not available':
            return {'success': False, 'error': 'Sync not available'}
        result = self._download_result()
        result['error'] = state.get('error')
        if state.get('stopped'):
            result['stopped'] = True
        result['notes_differing'] = count_differing_notes(self.lists_manager.data)
        return result

    def fetch_cloud_state(self, remembered_ids, should_stop=None, progress=None) -> Dict[str, Any]:
        """The network half of a download: reads the cloud, reads and writes nothing local.

        remembered_ids: ListsManager.remembered_row_ids(user), taken on the thread that owns
        the store. Returns the state for apply_cloud_state -- with 'success', 'stopped', and
        the fetch's pass under 'pass' (the apply needs what the confirmation found) -- or
        {'success': False, 'error', 'stopped'?}.
        """
        if not self.is_sync_available():
            return {'success': False, 'error': 'Sync not available'}
        if not self._sync_lock.acquire(blocking=False):
            return {'success': False, 'error': 'Sync already in progress'}
        try:
            return self._fetch(remembered_ids, should_stop, progress)
        finally:
            self._sync_lock.release()

    def _fetch(self, remembered_ids, should_stop=None, progress=None):
        client = self._get_client()
        if not client:
            return {'success': False, 'error': 'No Supabase client'}
        pass_ = _Pass(client, self._user_id, should_stop=should_stop)
        session = _session_user(client)
        if session is not None and session != str(pass_.user_id):
            logger.warning("The session belongs to another user than the one lists sync was set up for")
            return {'success': False, 'error': 'Sync not available'}
        pass_.session_before = session
        try:
            state = self._fetch_cloud_state(pass_, list(remembered_ids or ()), progress)
        except _Stopped:
            logger.info("Download stopped before its next request")
            return {'success': False, 'error': 'Sync stopped', 'stopped': True}
        except Exception as e:
            logger.error(f"Error syncing from cloud: {e}")
            return {'success': False, 'error': str(e)}
        state.update(success=True, stopped=False)
        state['pass'] = pass_
        return state

    def apply_cloud_state(self, state) -> Dict[str, Any]:
        """The local half of a download, from fetch_cloud_state's state: snapshot, merge, save.

        Writes lists.pkl.pre-download first, and changes nothing when it cannot. A state
        fetched for another user than the one now set up is refused.
        """
        if not self._sync_lock.acquire(blocking=False):
            return {'success': False, 'error': 'Sync already in progress'}
        try:
            return self._apply(state)
        finally:
            self._sync_lock.release()

    def _apply(self, state):
        pass_ = state.get('pass') if isinstance(state, dict) else None
        if pass_ is None or not state.get('success') or pass_.user_id != self._user_id:
            return {'success': False, 'error': 'Sync not available'}
        # A download rewrites local notes, tags and memberships and merges
        # duplicate items, so it does not start without its snapshot.
        if not self._backup_local_data('download'):
            logger.error("Download cancelled: the local lists could not be backed up first")
            return {'success': False, 'error': DOWNLOAD_BACKUP_FAILED}
        result = self._download_result()
        store = self.lists_manager.data
        try:
            self._apply_cloud_state(pass_, store, state, result)
            self.lists_manager.save()
            self._last_sync = time.time()
            result['success'] = True
        except Exception as e:
            logger.error(f"Error syncing from cloud: {e}")
            result['error'] = str(e)
        result['notes_differing'] = count_differing_notes(store)
        return result

    def _fetch_cloud_state(self, pass_, remembered_ids, progress=None):
        """Everything a download needs from the cloud; reads nothing local and writes nothing."""
        cloud_projects, projects_complete = self._read_projects(pass_, '*')
        cloud_lists, lists_complete = self._read_user_lists(pass_, '*')
        rows_by_list = {}
        # Lists in the Trash are read too: a local list that takes one as its own keeps
        # its own Trash state, and when that is live its entries come from there.
        read = [lst for lst in sorted(cloud_lists, key=lambda cl: _id_key(cl['id']))
                if lst.get('name') != 'Recently Viewed']
        for n, lst in enumerate(read):
            rows, complete = self._fetch_list_rows(pass_, lst['id'])
            rows_by_list[lst['id']] = {'rows': rows, 'complete': complete}
            self._tell_progress({'tell': progress}, n + 1, len(read))
        self._confirm(pass_, remembered_ids)
        pass_.prove_auth()
        return {'user_id': pass_.user_id, 'projects': cloud_projects, 'projects_complete': projects_complete,
                'lists': cloud_lists,
                'lists_complete': lists_complete, 'has_page': pass_.has_page, 'rows_by_list': rows_by_list,
                'where': pass_.where, 'absent': pass_.absent, 'locate_ok': pass_.locate_ok,
                'auth_ok': pass_.auth_ok}

    def _apply_projects(self, store, cloud_projects):
        """Cloud projects mapped to local ones by name, as before."""
        cloud_project_to_local = {}
        local_projects = store.setdefault('projects', {})

        for cloud_proj in cloud_projects:
            cloud_proj_id = cloud_proj['id']
            cloud_proj_name = cloud_proj.get('name', '')

            # Find matching local project by name
            local_proj_id = None
            for pid, pdata in local_projects.items():
                if pdata.get('name') == cloud_proj_name:
                    local_proj_id = pid
                    break

            if local_proj_id:
                # Update existing project
                cloud_project_to_local[cloud_proj_id] = local_proj_id
                local_projects[local_proj_id]['cloud_id'] = cloud_proj_id
                if cloud_proj.get('color'):
                    local_projects[local_proj_id]['color'] = cloud_proj['color']
            else:
                # Create new local project
                import uuid
                new_proj_id = f"proj_{uuid.uuid4().hex[:8]}"
                local_projects[new_proj_id] = {
                    'name': cloud_proj_name,
                    'color': cloud_proj.get('color', '#4CAF50'),
                    'created': time.time(),
                    'cloud_id': cloud_proj_id
                }
                cloud_project_to_local[cloud_proj_id] = new_proj_id
                store.setdefault('projects_order', []).append(new_proj_id)
        return cloud_project_to_local

    @staticmethod
    def _cloud_deleted_at(cloud_list):
        # Handle soft delete - convert cloud timestamp to local format
        cloud_deleted_at = cloud_list.get('deleted_at')
        local_deleted_at = None
        if cloud_deleted_at:
            # Parse ISO timestamp from cloud to Unix timestamp
            try:
                from datetime import datetime
                if isinstance(cloud_deleted_at, str):
                    dt = datetime.fromisoformat(cloud_deleted_at.replace('Z', '+00:00'))
                    local_deleted_at = dt.timestamp()
                else:
                    local_deleted_at = cloud_deleted_at
            except Exception:
                local_deleted_at = time.time()  # Fallback to now
        return local_deleted_at

    def _map_lists(self, store, cloud_lists, cloud_project_to_local, result, lists_complete=True):
        """Each local list's own cloud list, and the same-name cloud lists read for it.

        At most one local owner per cloud list. A list owns the cloud list whose id
        it holds, whatever either is called now: it takes the name the website gave
        that list, unless it was renamed here since an upload's write of its name
        returned the row (LIST_NAME_UNSENT) -- then its own name stands, for that
        upload to send. A list that holds no id takes by name, as before ('General'
        is the default list), the lowest-id cloud list of its name no local list
        holds -- the upload makes the same choice -- and, under the same exception,
        that list's name (only the default list's can differ). Only the own cloud list sets
        colour, project and the Trash state -- and not a part of it that is unsent
        (LIST_STATE_UNSENT: changed on this computer; or all of it, when the list held no
        cloud id and took the cloud list here): that part keeps its own value until an
        upload has sent it, and stays marked only while it differs from the cloud list's.
        A rename here never holds these back, and an unsent state never holds back a name.

        When the read of the lists was not complete (lists_complete False), a list whose
        cloud id the read did not return keeps that id and gets no cloud list this pass
        (its own may lie past what was read); a list that holds no id still takes a
        same-name one that was read, as the upload does.
        """
        local_lists = store.setdefault('lists', {})
        order = [lid for lid in _list_order(store) if _syncable_list(lid, local_lists[lid])]
        cloud_by_id = {}
        for cl in cloud_lists:
            if cl.get('name') == 'Recently Viewed':
                logger.debug("Skipping cloud list 'Recently Viewed' - Recently Viewed is local-only")
                continue
            cloud_by_id[cl['id']] = cl

        def candidate(cl, lid):
            name = cl.get('name', '')
            return local_lists[lid].get('name') == name or (lid == 'default' and name == 'General')

        held = {}
        for lid in order:
            cid = local_lists[lid].get('cloud_id')
            if cid is None or cid not in cloud_by_id:
                continue
            if cid in held:
                local_lists[lid].pop('cloud_id', None)
            else:
                held[cid] = lid
        had_own_id = {lid for cid, lid in held.items()}

        def take_name(ld, cid):
            # the cloud list's name, unless this list was renamed here (LIST_NAME_UNSENT)
            name = cloud_by_id[cid].get('name', '')
            if ld.get('name') != name and not ld.get(LIST_NAME_UNSENT):
                ld['name'] = name          # renamed on the website (or 'General' for the default list)
                if 'name_en' in ld:
                    ld['name_en'] = name

        own = {}
        for lid in order:
            ld = local_lists[lid]
            cid = ld.get('cloud_id')
            if cid in cloud_by_id and held.get(cid) == lid:
                own[lid] = cid
                take_name(ld, cid)
        taken = set(own.values())
        pending = [lid for lid in order if lid not in own]
        pending.sort(key=lambda lid: 0 if local_lists[lid].get('cloud_id') is not None else 1)
        for lid in pending:
            stored = local_lists[lid].get('cloud_id')
            if stored is not None and not lists_complete:
                logger.info("The cloud lists were not all read; list '%s' keeps cloud list %s and is not "
                            "updated until the next download", local_lists[lid].get('name'), stored)
                continue
            holding = {local_lists[x].get('cloud_id') for x in local_lists if x != lid}
            free = sorted((cid for cid, cl in cloud_by_id.items()
                           if candidate(cl, lid) and cid not in taken and cid not in holding), key=_id_key)
            if free:
                own[lid] = free[0]
                taken.add(free[0])
                local_lists[lid]['cloud_id'] = free[0]
                take_name(local_lists[lid], free[0])

        for lid, cid in own.items():
            cloud_list = cloud_by_id[cid]
            ld = local_lists[lid]
            cloud_project_id = cloud_list.get('project_id')
            local_project_id = cloud_project_to_local.get(cloud_project_id) if cloud_project_id else None
            local_deleted_at = self._cloud_deleted_at(cloud_list)
            differs = set()
            if bool(ld.get('deleted_at')) != bool(local_deleted_at):
                differs.add('trash')
            if cloud_list.get('color') and cloud_list['color'] != ld.get('color'):
                differs.add('color')
            if ((local_project_id and local_project_id != ld.get('project_id'))
                    or (not cloud_project_id and ld.get('project_id'))):
                differs.add('project')
            # a list that took its cloud list here keeps all of its own state; a list that
            # held that cloud list keeps the parts changed here since the last upload
            held = _held_state(ld) if lid in had_own_id else set(LIST_STATE_FIELDS)
            if 'color' not in held and cloud_list.get('color'):
                ld['color'] = cloud_list['color']
            # Update project assignment if changed
            if 'project' not in held and local_project_id:
                ld['project_id'] = local_project_id
            # Sync deleted_at status
            if 'trash' not in held or not differs & {'trash'}:
                if local_deleted_at:
                    ld['deleted_at'] = local_deleted_at   # (in the Trash on both sides: the cloud's time)
                elif 'deleted_at' in ld:
                    # Cloud restored the list - remove local deleted_at
                    del ld['deleted_at']
            still = held & differs
            if still:
                ld[LIST_STATE_UNSENT] = sorted(still)    # these parts go up with the next upload
            else:
                ld.pop(LIST_STATE_UNSENT, None)
            result['lists_updated'] += 1

        same_name = collections.defaultdict(list)
        for cid in sorted(cloud_by_id, key=_id_key):
            if cid in taken:                # every cloud list a local list holds is taken
                continue
            cl = cloud_by_id[cid]
            first = next((lid for lid in order if candidate(cl, lid)), None)
            if first is not None:
                same_name[first].append(cid)
                continue
            # Create new local list
            import uuid
            new_id = f"list_{uuid.uuid4().hex[:8]}"
            cloud_project_id = cl.get('project_id')
            local_project_id = cloud_project_to_local.get(cloud_project_id) if cloud_project_id else None
            new_list_data = {
                'name': cl.get('name', ''),
                'name_en': cl.get('name_en', cl.get('name', '')),
                'color': cl.get('color', '#FFD700'),
                'created': time.time(),
                'is_default': cl.get('is_default', False),
                'is_system': cl.get('is_system', False),
                'project_id': local_project_id,
                'cloud_id': cid
            }
            local_deleted_at = self._cloud_deleted_at(cl)
            if local_deleted_at:
                new_list_data['deleted_at'] = local_deleted_at
            local_lists[new_id] = new_list_data
            store.setdefault('lists_order', []).append(new_id)
            own[new_id] = cid
            result['lists_added'] += 1
        return own, same_name

    def _apply_cloud_state(self, pass_, store, state, result):
        """The local half of a download: map lists, fold rows into items, reconcile notes once per item."""
        self._guard(store, pass_)
        pass_.has_page = state['has_page']
        items = store.setdefault('items', {})
        cloud_project_to_local = self._apply_projects(store, state['projects'])
        cloud_lists = state['lists']
        lists_complete = state['lists_complete']
        cloud_list_ids = {cl['id'] for cl in cloud_lists}
        local_lists = store.setdefault('lists', {})
        if not cloud_lists and not state['projects']:
            logger.info("Cloud has no lists or projects - preserving local data unchanged")
            self._sweep_unowned(pass_, store, {}, lists_complete, cloud_list_ids, result)
            _drop_left_tombstones(items)
            return

        own, same_name = self._map_lists(store, cloud_lists, cloud_project_to_local, result, lists_complete)
        cloud_trashed = {cl['id']: bool(cl.get('deleted_at')) for cl in cloud_lists}
        has_page = bool(pass_.has_page)
        self._repair_shared_cloud_rows(store, merge=True, has_page=has_page)
        # A membership's record whose row is now in another list than the membership's own
        # (moved on another computer; a list remapped by name) is stale before any pairing.
        for iid, it in list(items.items()):
            for key in list(it.get('lists') or []):
                rec = _live_record(it, key)
                loc = pass_.where.get(rec['id']) if rec else None
                if loc is not None and key in own and loc[0] != own[key]:
                    _drop_record(it, key)
        orphan_ids = _orphan_ids(items)
        orphan_of = {}
        for iid, it in list(items.items()):
            for key, rec in _orphans(it):
                orphan_of[rec['id']] = (iid, key)

        # A row this account waits to delete (the user removed its entry here) is left out of
        # the cloud list its removal names: it re-adds no membership, creates no item and folds
        # no text. Found in another list, another computer moved it there, and its move wins:
        # that list takes it as the upload would, and the removal is over.
        pass_.pending_rows = self._suppressed(pass_, store)
        # Rows the confirmation alone located are handled as if their list's read had returned them.
        rows_by_list = {cid: list(entry['rows']) for cid, entry in state['rows_by_list'].items()}
        for rid in sorted(pass_.confirmed_only, key=_id_key):
            loc = pass_.where[rid]
            if loc[0] in rows_by_list:
                rows_by_list[loc[0]].append(pass_.rows_by_id.get(rid) or
                                            {'id': rid, 'list_id': loc[0], 'note': loc[1], 'tags': loc[2]})

        sources = collections.defaultdict(list)   # item id -> [(row id, note, tags, base, record)]
        pending_records = []                       # (item id, list key, row id, cloud list id)
        by_sys = collections.defaultdict(list)
        for iid, it in list(items.items()):
            by_sys[str(it.get('sys_id') or str(iid).split('::', 1)[0])].append(iid)

        for list_id in [lid for lid in _list_order(store) if lid in own]:
            ld = local_lists[list_id]
            if ld.get('deleted_at') or not _syncable_list(list_id, ld):
                continue
            cloud_id = own[list_id]
            if cloud_id not in rows_by_list:
                continue
            read = [cloud_id] + [cid for cid in same_name.get(list_id, [])
                                 if cid in rows_by_list and not cloud_trashed.get(cid)]
            matched_own = self._walk_list(pass_, items, list_id, cloud_id, read, rows_by_list, orphan_of,
                                          orphan_ids, by_sys, sources, pending_records, has_page, result)
            # members of this list whose remembered row was not seen in it
            complete = state['rows_by_list'][cloud_id]['complete']
            for iid, it in list(items.items()):
                if list_id not in (it.get('lists') or []) or iid in matched_own:
                    continue
                if is_local_sys_id(it.get('sys_id', iid)):
                    continue
                rec = _live_record(it, list_id)
                if rec is None:
                    continue
                self._judge_unseen(pass_, iid, it, list_id, rec, cloud_id, complete, lists_complete,
                                   cloud_list_ids, result)

        self._sweep_unowned(pass_, store, own, lists_complete, cloud_list_ids, result)

        # The row of a Move or removal made here: whatever list it is in now (moved there,
        # or in a list in the Trash), its note and tags reach the entry against its bases.
        for rid in sorted(orphan_of, key=_id_key):
            oiid, okey = orphan_of[rid]
            loc = pass_.where.get(rid)
            it = items.get(oiid)
            if loc is None or it is None:
                continue
            orec = (it.get('cloud_rows') or {}).get(okey)
            if isinstance(orec, dict) and orec.get('id') == rid:
                sources[oiid].append((rid, loc[1], loc[2], dict(orec), (okey, loc[0])))

        for iid, srcs in sources.items():
            if iid in items:
                self._reconcile(items[iid], srcs, result)
        for iid, list_id, rid, cloud_id in pending_records:
            it = items.get(iid)
            if it is None:
                continue
            src = next((s for s in sources.get(iid, ()) if s[0] == rid), None)
            note = (src[1] or '') if src else None
            tags = list(src[2] or []) if src else None
            _remember(pass_, store, iid, it, list_id, rid, cloud_id, note, tags)
        for iid, srcs in sources.items():
            it = items.get(iid)
            if it is None:
                continue
            for rid, note, tags, base, orphan in srcs:
                if orphan is None:
                    continue
                okey, cid = orphan
                rec = (it.get('cloud_rows') or {}).get(okey)
                if isinstance(rec, dict) and rec.get('id') == rid and not rec.get('gone'):
                    rec['note'] = note or ''
                    rec['tags'] = list(tags or [])
                    rec['list'] = pass_.where.get(rid, (cid,))[0]
                    rec.pop('differs', None)
        _drop_left_tombstones(items)

    def _walk_list(self, pass_, items, list_id, cloud_id, read, rows_by_list, orphan_of, orphan_ids, by_sys,
                   sources, pending_records, has_page, result):
        """The rows of one local list's own and same-name cloud lists: pair, fold or create.

        Planned again (at most twice more) when the plan creates an item or brings one into
        the list: as a member it may be the better pair for a row of the own list, and the
        next Download would pair them so -- the plan that changes no membership is applied.
        Returns the items paired with a row of the own list.
        """
        created_all, joined_all = set(), set()
        for attempt in range(3):
            claimed = set(pass_.claimed)
            plan, created = self._plan_list(items, list_id, cloud_id, read, rows_by_list, orphan_of, orphan_ids,
                                            by_sys, has_page, claimed, pass_.pending_rows)
            created_all |= created
            joined = {iid for _, _, iid, _ in plan['matched'] if list_id not in (items[iid].get('lists') or [])}
            if not (created or joined) or attempt == 2:
                break
            for iid in joined:
                items[iid].setdefault('lists', []).append(list_id)   # a member while planned again
            joined_all |= joined
        kept = ({iid for _, _, iid, _ in plan['matched']} | {iid for _, iid, _ in plan['folds']}
                | {iid for _, _, iid, _ in plan['creates']})
        for iid in created_all - kept:
            items.pop(iid, None)          # created by an earlier plan; the final one pairs its row elsewhere
        for iid in joined_all - kept:
            if iid in items and list_id in (items[iid].get('lists') or []):
                items[iid]['lists'].remove(list_id)   # joined for an earlier plan only
        pass_.claimed = claimed
        matched_own = set()
        for cid, rid, iid, row in plan['matched']:
            it = items[iid]
            if list_id not in it.setdefault('lists', []):
                it['lists'].append(list_id)
                joined_all.add(iid)
            if cid == cloud_id:
                sources[iid].append((rid, row.get('note'), row.get('tags'), _base_of(it, rid), None))
                pending_records.append((iid, list_id, rid, cloud_id))
                matched_own.add(iid)
            else:
                sources[iid].append((rid, row.get('note'), row.get('tags'), None, None))
        for rid, iid, row in plan['folds']:
            sources[iid].append((rid, row.get('note'), row.get('tags'), None, None))
        for cid, rid, iid, row in plan['creates']:
            sources[iid].append((rid, row.get('note'), row.get('tags'), None, None))
            if cid == cloud_id:
                pending_records.append((iid, list_id, rid, cloud_id))
                matched_own.add(iid)
        result['items_added'] += len(created_all & kept) + len((joined_all & kept) - created_all)
        return matched_own

    def _plan_list(self, items, list_id, cloud_id, read, rows_by_list, orphan_of, orphan_ids, by_sys, has_page,
                   claimed, pending=None):
        used = set()
        matched_by_list = {}
        for cid in read:
            rows = []
            waiting = (pending or {}).get(cid, ())
            for row in rows_by_list[cid]:
                rid = row.get('id')
                if rid in orphan_of:
                    continue    # an orphan's row: folded wherever it is; never re-added
                if rid in waiting:
                    continue    # its entry was removed from this list here: it re-adds and folds nothing
                if row.get('sys_id') is not None and is_local_sys_id(row.get('sys_id')):
                    continue
                rows.append(row)
            sys_ids = {str(r['sys_id']) for r in rows if r.get('sys_id') is not None}
            cands = [(iid, items[iid]) for sid in sys_ids for iid in by_sys.get(sid, ()) if iid in items]
            cands += [(iid, it) for iid, it in list(items.items())
                      if any(_base_of(it, r.get('id')) for r in rows if r.get('sys_id') is None)]
            seen = set()
            cands = [c for c in cands if not (c[0] in seen or seen.add(c[0]))]
            members = [c for c in cands if list_id in (c[1].get('lists') or [])]
            # An entry moved from this list here (its row there is on its way to another
            # list) is not brought back by another row of it: that row becomes an entry
            # of its own.
            others = [c for c in cands if list_id not in (c[1].get('lists') or [])
                      and _live_record(c[1], list_id) is None
                      and not any(_is_move_key(k) and r.get('list') in read for k, r in _records(c[1])
                                  if not r.get('gone'))]

            # A second row of an entry this list already holds folds into it (below)
            # rather than bringing another local item of that entry into the list.
            def not_ours(row, used=used, loose=not has_page and cid != cloud_id):
                theirs = _row_identity(row, has_page)
                return not any(_same_entry(_item_identity(i, items[i]), theirs, has_page)
                               or (loose and _item_identity(i, items[i])[:2] == theirs[:2])
                               for i in used if i in items)

            n = 3 if has_page else 2
            exact = {_item_identity(i, it)[:n] for i, it in members}

            def not_its_twin(row, used=used, n=n, exact=exact, loose=not has_page):
                theirs = _row_identity(row, has_page)
                paired = [(_item_identity(i, items[i]), items[i]) for i in used if i in items]
                if any(mine[:n] == theirs[:n] for mine, _ in paired):
                    return False
                if theirs[:n] in exact:
                    return True     # the entry of this list with its exact identity takes it
                return not any((_same_entry(mine, theirs, has_page) or (loose and mine[:2] == theirs[:2]))
                               and _holds(it.get('note'), row.get('note'))
                               and _tagset(row.get('tags')) <= _tagset(it.get('tags'))
                               for mine, it in paired)

            # A same-name list's rows are never recorded: a row with the exact identity of an
            # item already paired is not taken by another entry of this list, nor is a row no
            # entry of the list has the exact identity of that is the same entry as an item
            # already paired holding its note and tags; it folds into that item, as every
            # later Download will fold it.
            matched = _match_rows(rows, members, list_id, has_page, claimed, orphan_ids, used,
                                  content_ok=not_its_twin if cid != cloud_id else None)
            matched.update(_match_rows(rows, others, list_id, has_page, claimed, orphan_ids, used,
                                       content_ok=not_ours))
            matched_by_list[cid] = (rows, matched)
        plan = {'matched': [], 'folds': [], 'creates': []}
        for cid in read:
            rows, matched = matched_by_list[cid]
            by_id = {r.get('id'): r for r in rows}
            for rid, iid in sorted(matched.items(), key=lambda kv: _id_key(kv[0])):
                plan['matched'].append((cid, rid, iid, by_id[rid]))
        created = set()
        for cid in read:
            rows, matched = matched_by_list[cid]
            for row in sorted(rows, key=lambda r: _id_key(r.get('id'))):
                rid = row.get('id')
                if rid in matched or row.get('sys_id') is None:
                    continue
                claimed.add(rid)
                theirs = _row_identity(row, has_page)
                # A row of a same-name list is never recorded, so an item created from it
                # would be uploaded into the own list and read back here as another row:
                # without the page column it folds into an entry of the same folio instead.
                loose = not has_page and cid != cloud_id
                fold = [(iid, items[iid]) for iid in used if iid in items
                        and (_same_entry(_item_identity(iid, items[iid]), theirs, has_page)
                             or (loose and _item_identity(iid, items[iid])[:2] == theirs[:2]))]
                if fold:
                    # prefer the entry that already holds this row's note and tags (it keeps
                    # holding them), so the next Download folds the row into the same entry
                    def holds(c, row=row):
                        return (_holds(c[1].get('note'), row.get('note'))
                                and _tagset(row.get('tags')) <= _tagset(c[1].get('tags')))
                    iid = min(fold, key=lambda c: (list_id not in (c[1].get('lists') or []), not holds(c),
                                                   c[1].get('added') or 0, str(c[0])))[0]
                    plan['folds'].append((rid, iid, row))
                    continue
                iid = self._new_item(items, row, list_id, has_page)
                used.add(iid)
                by_sys[str(row['sys_id'])].append(iid)
                created.add(iid)
                plan['creates'].append((cid, rid, iid, row))
        return plan, created

    def _new_item(self, items, row, list_id, has_page):
        """An unmatched row that is no local entry's: a new item at a free key."""
        sys_id = str(row['sys_id'])
        fl_id = row.get('fl_id')
        page = _norm(row.get('page')) if has_page else None
        build = self.lists_manager._build_item_id
        iid = build(sys_id, img=page, fl_id=fl_id)
        if iid in items:
            iid = build(sys_id, fl_id=fl_id) if _norm(fl_id) else iid
            if iid in items:
                iid = f"{sys_id}::row::{row['id']}"
                n = 1
                while iid in items:   # an entry made from this row before, and kept
                    n += 1
                    iid = f"{sys_id}::row::{row['id']}::{n}"
        now = time.time()
        # its row's note and tags from the start, so later rows of this pass compare with them
        items[iid] = {
            'sys_id': sys_id,
            'lists': [list_id],
            'tags': list(row.get('tags') or []),
            'note': row.get('note') or '',
            'source': 'cloud_sync',
            'added': now,
            'modified': now,
            'shelfmark_override': None,
            'fl_id': fl_id,
            'img': page,
        }
        return iid

    def _judge_unseen(self, pass_, iid, it, list_id, rec, cloud_id, read_complete, lists_complete,
                      cloud_list_ids, result):
        """A remembered row of this membership that no read or confirmation paired with it."""
        rid = rec['id']
        if rid in pass_.where:
            if pass_.where[rid][0] != cloud_id:
                _drop_record(it, list_id)   # located elsewhere: stale
            return                          # in its own list (it was an orphan's row): kept
        if rec.get('list') != cloud_id and lists_complete and rec.get('list') in cloud_list_ids:
            _drop_record(it, list_id)       # its list is no longer this list's own
            return
        if self._may_tombstone(pass_, rec, cloud_id, read_complete, lists_complete, cloud_list_ids):
            self._tombstone(iid, it, list_id, result)
        else:
            result['unchecked'] += 1

    def _sweep_unowned(self, pass_, store, own, lists_complete, cloud_list_ids, result):
        """Memberships whose local list has no cloud list of its own in this pass (deleted there, or none yet).

        A list whose cloud id a read of the lists that was not complete did not return may still have
        that cloud list: its memberships keep their records and count as unchecked.
        """
        lists = store.get('lists') or {}
        for iid, it in list((store.get('items') or {}).items()):
            if is_local_sys_id(it.get('sys_id', iid)):
                continue
            for list_id in list(it.get('lists') or []):
                ld = lists.get(list_id)
                if ld is None or list_id in own or ld.get('deleted_at') or not _syncable_list(list_id, ld):
                    continue
                rec = _live_record(it, list_id)
                if rec is None:
                    continue
                if not lists_complete and ld.get('cloud_id') is not None and ld['cloud_id'] not in cloud_list_ids:
                    result['unchecked'] += 1   # its cloud list was not read: nothing is concluded this pass
                    continue
                self._judge_unseen(pass_, iid, it, list_id, rec, None, False, lists_complete, cloud_list_ids,
                                   result)

    def _reconcile(self, it, sources, result):
        """Every cloud value that reached this entry in this pass, applied at once, in row-id order.

        A source whose row changed from exactly the local value replaces it -- unless all
        it did was drop part of it while another row still holds the whole local value
        (two rows disagree; both are kept, as for any clash). When the local value is
        replaced, a row still holding it keeps it among the texts kept.
        """
        sources = sorted(sources, key=lambda s: _id_key(s[0]))
        x0 = it.get('note') or ''
        notes = [(c_note or '', base.get('note') if base else None) for _, c_note, _, base, _ in sources]
        still = any(_holds(c, x0) for c, _ in notes)
        superseded = any(b is not None and _same_text(x0, b) and not _same_text(c, b)
                         and not (still and _holds(x0, c)) for c, b in notes)
        new = []
        for c, b in notes:
            if (b is not None and _same_text(c, b)) or (_same_text(c, x0) and not superseded):
                continue
            if not any(_same_text(c, n) for n in new):
                new.append(c)
        parts = new if superseded else [x0] + new
        note = x0 if not new else _fold_notes(parts)

        xt = list(it.get('tags') or [])
        xs = _tagset(xt)
        tag_sources = [(list(c_tags or []), base.get('tags') if base else None)
                       for _, _, c_tags, base, _ in sources]
        tstill = any(_tagset(ct) >= xs for ct, _ in tag_sources)
        tsuper = any(bt is not None and xs == _tagset(bt) and _tagset(ct) != _tagset(bt)
                     and not (tstill and _tagset(ct) <= xs) for ct, bt in tag_sources)
        tnew = []
        for ct, bt in tag_sources:
            if (bt is not None and _tagset(ct) == _tagset(bt)) or (_tagset(ct) == xs and not tsuper):
                continue
            if all(_tagset(ct) != _tagset(t) for t in tnew):
                tnew.append(ct)
        tparts = tnew if tsuper else [xt] + tnew
        tags = xt
        if tnew:
            tags = []
            for t in tparts:
                tags = _union(tags, t)
        # a note merge wrote a marker line: the note is a new combination of the texts;
        # combined tags are counted apart (they leave no marker)
        if bool(new) and len(parts) >= 2 and note not in parts and note != x0:
            result['notes_merged'] += 1
        if bool(tnew) and len(tparts) >= 2 and _tagset(tags) != xs \
                and all(_tagset(tags) != _tagset(t) for t in tparts):
            result['tags_merged'] += 1
        it['note'] = note
        it['tags'] = tags

    # ------------------------------------------------------------------
    # The single-list and single-item helpers
    # ------------------------------------------------------------------

    def sync_list_to_cloud(self, list_id: str) -> bool:
        """Push a specific list and its items to cloud."""
        # ===== Phase 95 LOCAL gate (D-30 Codex P0, REQ-9) =====
        # Abort entire list sync if any item belonging to this list has a LOCAL sys_id.
        # B2 — field names pinned from sync_to_cloud:619-635 canonical pattern.
        # Items are stored as a flat dict at self.lists_manager.data['items'].
        # Each item dict has a 'lists' list field holding the list_ids it belongs to.
        # The sys_id is in 'sys_id' (fallback item_id).
        items_map = self.lists_manager.data.get('items', {})
        for iid, item_data in items_map.items():
            if list_id not in (item_data.get('lists') or []):
                continue  # item not in this list
            if is_local_sys_id(item_data.get('sys_id', iid)):
                logger.info("[list contains LOCAL items, not synced] list_id=%s", list_id)
                return False
        # ======================================================
        if not self.is_sync_available():
            return False

        try:
            client = self._get_client()
            if not client:
                return False

            list_data = self.lists_manager.data.get('lists', {}).get(list_id)
            if not list_data or list_data.get('is_system'):
                return False

            cloud_id = list_data.get('cloud_id')

            list_payload = {
                'user_id': self._user_id,
                'name': list_data.get('name', 'Unnamed'),
                'name_en': list_data.get('name_en', list_data.get('name', '')),
                'color': list_data.get('color', '#FFD700'),
                'is_default': list_data.get('is_default', False),
                'is_system': False
            }

            if cloud_id:
                # as the upload: another name in the cloud was given on the website (or by
                # another computer) unless this list was renamed here (LIST_NAME_UNSENT).
                # LIST_STATE_UNSENT stays for the upload: this sends no project or Trash state.
                if not list_data.get(LIST_NAME_UNSENT):
                    del list_payload['name'], list_payload['name_en']
                response = client.table('user_lists').update(list_payload).eq('id', cloud_id).execute()
            else:
                response = client.table('user_lists').insert(list_payload).execute()
                if response.data:
                    list_data['cloud_id'] = response.data[0]['id']
            if response.data and 'name' in list_payload and list_data.get('name') == list_payload['name']:
                list_data.pop(LIST_NAME_UNSENT, None)   # the row answered with the name sent
            if response.data:
                self.lists_manager.save()

            return True

        except Exception as e:
            logger.error(f"Error syncing list to cloud: {e}")
            return False

    def sync_item_to_cloud(self, item_id: str, list_id: str) -> bool:
        """Push one membership (an item in one list) to cloud, as an upload would."""
        # ===== Phase 95 LOCAL gate (D-30 Codex P0 + HIGH-2 review fix, REQ-9) =====
        # MUST run BEFORE _get_client() and sync_list_to_cloud() — both leak
        # cloud activity even though the natural sys_id lookup is at line ~762.
        # HIGH-2: derive sys_id BEFORE the `if item_data:` branch so a LOCAL
        # item_id with missing item_data is ALSO gated (the previous draft
        # nested the derivation INSIDE the `if item_data:` body which let this
        # case slip through).
        # Lookup from in-memory self.lists_manager.data only (no network).
        item_data = self.lists_manager.data.get('items', {}).get(item_id)
        sys_id = item_data.get('sys_id', item_id) if item_data else item_id
        if is_local_sys_id(sys_id):
            logger.info("[local-only item, not synced] item_id=%s sys_id=%s", item_id, sys_id)
            return False
        # ===========================================================================
        if not self.is_sync_available():
            return False

        try:
            client = self._get_client()
            if not client:
                return False

            list_data = self.lists_manager.data.get('lists', {}).get(list_id)
            if not list_data:
                return False

            cloud_list_id = list_data.get('cloud_id')
            if not cloud_list_id:
                # Need to sync list first
                self.sync_list_to_cloud(list_id)
                cloud_list_id = list_data.get('cloud_id')
                if not cloud_list_id:
                    return False

            item_data = self.lists_manager.data.get('items', {}).get(item_id)
            if not item_data or list_id not in (item_data.get('lists') or []):
                return False
        except Exception as e:
            logger.error(f"Error syncing item to cloud: {e}")
            return False

        if not self._sync_lock.acquire(blocking=False):
            return False
        try:
            result = self._upload(self.lists_manager.data, client, only_list=list_id, only_item=item_id)
            return bool(result.get('success'))
        except Exception as e:
            logger.error(f"Error syncing item to cloud: {e}")
            return False
        finally:
            self._sync_lock.release()

    def delete_list_from_cloud(self, list_id: str) -> bool:
        """Delete a list from cloud (cascade deletes items)."""
        if not self.is_sync_available():
            return False

        try:
            client = self._get_client()
            if not client:
                return False

            list_data = self.lists_manager.data.get('lists', {}).get(list_id)
            if not list_data:
                return True  # Already deleted locally

            cloud_id = list_data.get('cloud_id')
            if cloud_id:
                client.table('user_lists').delete().eq('id', cloud_id).execute()

            return True

        except Exception as e:
            logger.error(f"Error deleting list from cloud: {e}")
            return False

    def delete_item_from_cloud(self, item_id: str, list_id: str) -> bool:
        """Delete the one cloud row remembered for this item in this list; nothing else."""
        if not self.is_sync_available():
            return False
        store = self.lists_manager.data
        item_data = (store.get('items') or {}).get(item_id)
        rec = _live_record(item_data, list_id) if item_data else None
        if rec is None or store.get('cloud_account') != self._user_id:
            logger.info("delete_item_from_cloud: no remembered row for %s in %s; nothing deleted", item_id, list_id)
            return False
        if not self._sync_lock.acquire(blocking=False):
            return False
        try:
            client = self._get_client()
            if not client:
                return False
            client.table('list_items').delete().eq('id', rec['id']).eq('list_id', rec['list']).execute()
            _drop_record(item_data, list_id)
            self.lists_manager.save()
            return True
        except Exception as e:
            logger.error(f"Error deleting item from cloud: {e}")
            return False
        finally:
            self._sync_lock.release()


# Singleton instance
_sync_instance: Optional[ListsCloudSync] = None


def get_lists_sync(lists_manager=None) -> ListsCloudSync:
    """Get or create the lists sync singleton."""
    global _sync_instance
    if _sync_instance is None:
        _sync_instance = ListsCloudSync(lists_manager)
    elif lists_manager is not None and _sync_instance.lists_manager is None:
        _sync_instance.lists_manager = lists_manager
    return _sync_instance
