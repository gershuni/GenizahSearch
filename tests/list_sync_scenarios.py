# -*- coding: utf-8 -*-
"""Seeded, randomized scenarios for the desktop list sync, checked against invariants.

Two desktops -- each a real ListsManager with a real ListsCloudSync -- and the
website share one stateful stand-in for Supabase's PostgREST API. A seed draws a
sequence of operations: user edits on either desktop, website edits, uploads,
downloads and merges, with failures injected into chosen requests. After every
step the invariants below are checked; at the end both desktops sync until
nothing changes (the settle). A failing seed is shrunk to a short op list and
printed as a literal for REGRESSION_CASES in tests/test_list_sync_scenarios.py.

    python tests/list_sync_scenarios.py --seeds 0-499 --steps 60
    python tests/list_sync_scenarios.py --seeds 0-19999 --steps 60 --jobs 8
    python tests/list_sync_scenarios.py --engine fixture --seeds 0-49 --shrink

--engine is 'current' (shared/lists_sync.py), 'fixture' (the engine as it was
before per-membership records: tests/fixtures/lists_sync_816ccf7c.py.txt) or the
path of a variant of lists_sync.py.

A desktop syncs as its runner does (desktop/lists_sync_runner.py), when the
ListsManager and the engine offer that path: an upload runs on a copy of the store
(begin_upload, sync_to_cloud(data=copy), finish_upload) while the user may edit the
live store, and a download fetches, then applies. The fixture engine, and a tree
without that path, sync the live store directly.

Invariants (after every step unless stated):
  1  user text is never lost: every live note line and tag is in some copy (an
     explicit removal's DELETE retires what it took; a conditional one retires nothing)
  2  a row changes list only by a desktop's orphan move; identities only fill
  3  nothing is deleted except explicit removals the user made or accepted: every
     desktop DELETE is explicit (a pending removal of the pass's account, filtered by
     id and its list alone, that a removal in the ledger of 11 explains) or redundant
     (a moved row's, filtered by the note and tags it holds, which its entry holds);
     no row vanishes during a step but by such a DELETE or the website; a pass drops
     no local membership
  4  a row is named by one record, and never by a record and a pending removal at
     once; a desktop never inserts a row it should have claimed, nor an entry it
     already inserted in the same step (unless that row was recorded for one
     membership of the entry and this insert is for another that has none); the
     settle reaches a fixed point
  5  a second identical successful sync changes nothing
  6  My Library (97...) sys_ids never reach the cloud or come back from it
  7  records and pending removals carry their account; no DELETE for another account's
  8  at the settle every membership has a record naming a row of its own list
  9  at the settle every cloud row reaches a local item that is the same entry
 10  list names, against the harness's own record of desktop renames (never the
     engine's flag): a rename made on a desktop is pending until an upload's write of
     that name to the list's cloud list was answered with the row (the record follows
     lists.pkl through saves, restarts and backup recoveries); a Download gives every
     list without a pending rename the name of the cloud list it holds, and never
     changes a list with one; an upload writes a name over another only for a list with
     a pending rename of that name; at the settle no rename is pending and every list
     has its cloud list's name -- so a website rename reaches every desktop and no name
     flips back
 11  removals reach the website: every explicit removal (remove, delete permanently,
     empty the Trash, a prompt's Remove) is a ledger entry whose expected rows come from
     nothing the sync's bookkeeping wrote -- the membership record just before it, the
     harness's own map of moves, and the fake's log of what the upload running then
     inserted or moved for that entry; at the settle none of them exists, except the
     designed residuals (a request whose answer never came back, a killed upload, a row
     another actor moved to another list, another account's, and an entry the user put
     back in that list before its delete went)
 12  a Download never re-adds a membership only rows pending deletion justify
 13  the website-removal prompt is offered only when the harness's own written-out
     gate allows it
 14  an edit made during an upload counts as made after it: the step replayed with the
     edit right after the upload settles to the same website and desktops (steps in
     which the design's one difference can delete a live row are excluded)
  R  rule checks on every request and around every pass (_check_request, _after_pass)

Nothing here talks to a network or writes a file: saves and snapshots stay in
memory, and time.time/uuid.uuid4 are seeded for the length of a run.
"""
import argparse
import collections
import copy
import importlib
import importlib.util
import inspect
import json
import logging
import os
import pickle
import random
import sys
import tempfile
import time
import types
import urllib.parse
import uuid
from contextlib import contextmanager

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import httpx  # noqa: E402
from postgrest.exceptions import APIError  # noqa: E402

from shared import lists_manager as lm_mod  # noqa: E402

FIXTURE_PATH = os.path.join(HERE, 'fixtures', 'lists_sync_816ccf7c.py.txt')

SYS = ('990001', '990002', '990003')
LOCAL_SYS = '970000000000000001'      # a My Library entry a desktop adds
SEEDED_LOCAL_SYS = '970000000000000002'  # a My Library row already in the cloud (legacy)
SHAPES = ((None, None), ('1', None), ('2', None), ('1', 'FLa'), ('2', 'FLb'), (None, 'FLa'), ('1', 'FLb'))
NAMES = ('L1', 'L2', 'L3')
USER, OTHER_USER = 'u1', 'u2'
LONG_FILLER = 'L' * 9000
# Two of the four draws that were web_between_pages are web_churn_same_count, so every
# other injection a seed draws is the one it drew before.
INJECTIONS = ('raise_before', 'raise_after', 'api_error', 'anon', 'session_lost', 'web', 'web',
              'other_desktop_pass', 'same_desktop_edit', 'url_too_long',
              'web_between_pages', 'web_between_pages', 'web_churn_same_count', 'web_churn_same_count',
              'web_rename', 'between_stages')
# Of the desktop edits a seed draws, the share that are instead one of the ops the
# runner brings (drawn from a stream of their own), and their relative weights.
RUNNER_OP_SHARE = 0.342
RUNNER_OPS = (('prompt', 4.0), ('signout', 2.0), ('close', 1.0), ('kill', 0.5), ('offline', 1.0), ('ui', 2.0))
# The window's state a desktop starts with: what the website-removal prompt waits on.
UI_DEFAULT = {'visible': True, 'restoring': False, 'modal_open': False, 'logout_pending': False,
              'close_pending': False}
# Of the website ops a seed draws, the share that rename a list; of the desktop ops, the
# share that do (drawn from a stream of their own, so the other ops keep their values).
WEB_RENAME_SHARE = 0.125
DESK_RENAME_SHARE = 0.0625
# Result errors of a pass that returned before touching anything.
EARLY_ERRORS = ('Sync not available', 'Sync already in progress', 'No Supabase client')
# The only keys of a store an upload may change: the design's list, fixed here rather than
# read from the engine under test (a list's unsent-state and unsent-name flags are cleared
# once they are sent; a pending removal is sent or found gone).
IDENTITY_FIELDS = {'store': ('cloud_account', 'cloud_deletes'), 'projects': ('cloud_id',),
                   'lists': ('cloud_id', 'list_state_unsent', 'list_name_unsent'),
                   'items': ('cloud_id', 'cloud_rows')}
# The mark today's ListsManager.update_list puts on a renamed list. No check reads it; it
# is only taken off again while a desktop runs the pre-2b engine, whose ListsManager set no
# such mark (so the upgrade starts from a store as v9.3.0 left it).
RENAME_MARK = 'list_name_unsent'
# Errors after which the request's answer never reaches the engine (the write may have landed).
UNANSWERED = ('raise_after', 'api_pgrst111', 'api_504', 'gone_after')
ALL_CHECKS = frozenset({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 'R', 'crash'})
# the same-desktop edit that removes the entry whose row the upload wrote last (the self-test mix)
LAST_WRITE = ('last-write',)
# desk_op kinds whose lost memberships are moves (into another list) and removals (the ledger of 11)
MOVE_KINDS = (4, 8)
REMOVAL_KINDS = (3, 9, 11)

_NOWHERE = os.path.join(tempfile.gettempdir(), 'genizah-list-sync-scenarios-never-written')


class _ProcessGone(BaseException):
    """The program closed, or was killed, in the middle of a request (the engine catches only Exception)."""


_SIGNATURES = {}


def _takes(fn, name):
    """Whether fn accepts the keyword `name` (the 2b-1 engine and the fixture take none of the runner's)."""
    key = getattr(fn, '__func__', fn)
    params = _SIGNATURES.get(key)
    if params is None:
        try:
            params = _SIGNATURES[key] = frozenset(inspect.signature(fn).parameters)
        except (TypeError, ValueError):
            params = _SIGNATURES[key] = frozenset()
    return name in params


def _gate():
    """The runner's dialog gate, when the tree has one (None otherwise)."""
    try:
        from desktop.lists_sync_runner import sync_dialog_allowed  # noqa: PLC0415 - optional, and Qt-bound
    except Exception:
        return None
    return sync_dialog_allowed


# --------------------------------------------------------------------------- engines

_ENGINES = {}


def load_engine(name='current'):
    """The lists_sync module to test: 'current', 'fixture', or a path to a variant file."""
    if name in _ENGINES:
        return _ENGINES[name]
    if name == 'current':
        mod = importlib.import_module('shared.lists_sync')
    else:
        path = FIXTURE_PATH if name == 'fixture' else os.path.abspath(name)
        modname = 'lists_sync_fixture' if name == 'fixture' else 'lists_sync_variant_%d' % len(_ENGINES)
        mod = types.ModuleType(modname)
        mod.__file__ = path
        with open(path, encoding='utf-8') as fh:
            code = compile(fh.read(), path, 'exec')
        sys.modules[modname] = mod
        exec(code, mod.__dict__)
    _ENGINES[name] = mod
    return mod


@contextmanager
def _run_context(seed, engines):
    """Seeded clock and ids, quiet logging, and engines that believe Supabase is configured."""
    real_time, real_uuid4 = time.time, uuid.uuid4
    urng = random.Random(seed ^ 0xA11)
    clock = [1_800_000_000.0]

    def fake_uuid4():
        return uuid.UUID(int=urng.getrandbits(128))

    def fake_time():
        clock[0] += 1.0
        return clock[0]

    saved = [(m, m.SUPABASE_AVAILABLE, m.SUPABASE_ANON_KEY) for m in engines]
    prev_disable = logging.root.manager.disable
    time.time, uuid.uuid4 = fake_time, fake_uuid4
    logging.disable(logging.CRITICAL)
    for m in engines:
        m.SUPABASE_AVAILABLE, m.SUPABASE_ANON_KEY = True, 'scenario-key'
    _RUN['clock'], _RUN['urng'] = clock, urng
    try:
        yield
    finally:
        _RUN.clear()
        time.time, uuid.uuid4 = real_time, real_uuid4
        logging.disable(prev_disable)
        for m, avail, key in saved:
            m.SUPABASE_AVAILABLE, m.SUPABASE_ANON_KEY = avail, key


# the seeded clock and id stream of the run in progress, so a twin world can leave them as it found them
_RUN = {}


@contextmanager
def _same_clock():
    """Whatever runs inside leaves the seeded clock and ids where they were."""
    clock, urng = _RUN.get('clock'), _RUN.get('urng')
    state = (clock[0], urng.getstate()) if clock is not None else None
    try:
        yield
    finally:
        if state is not None:
            clock[0] = state[0]
            urng.setstate(state[1])


# --------------------------------------------------------------------------- identity and tokens

def norm(v):
    if v is None:
        return None
    t = str(v).strip()
    return t or None


def ident(iid, it):
    """(sys_id, fl_id, page) of a local item: fields first, then the parts of its key."""
    key = str(iid)
    page = norm(it.get('img'))
    if page is None and '::img::' in key:
        page = norm(key.split('::img::', 1)[1].split('::', 1)[0])
    fl = norm(it.get('fl_id'))
    if fl is None and '::fl::' in key:
        fl = norm(key.split('::fl::', 1)[1].split('::', 1)[0])
    return (str(it.get('sys_id') or key.split('::')[0]), fl, page)


def row_ident(r, has_page=True):
    return (str(r.get('sys_id')), norm(r.get('fl_id')), norm(r.get('page')) if has_page else None)


def same_entry(a, b, has_page):
    """Certainly one entry: the harness's own copy of the design's predicate."""
    if a[0] != b[0]:
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


def web_shelfmark(sys_id):
    """The catalogue shelfmark the website stores with a row it adds."""
    return f'T-S {sys_id[-3:]}.{int(sys_id[-1]) + 1}'


def web_title(sys_id):
    return f'Fragment {sys_id}'


def compatible(a, b):
    return a[0] == b[0] and not (a[1] and b[1] and a[1] != b[1]) and not (a[2] and b[2] and a[2] != b[2])


def toks(text, prefix='n'):
    return {ln for ln in (text or '').replace('\r\n', '\n').split('\n')
            if ln.startswith(prefix) and ln[1:].isdigit()}


def tag_toks(tags):
    return {t for t in (tags or []) if isinstance(t, str) and t.startswith('g') and t[1:].isdigit()}


# --------------------------------------------------------------------------- the fake PostgREST

LIST_ITEM_COLS = ('id', 'list_id', 'sys_id', 'shelfmark', 'title', 'fl_id', 'note', 'tags', 'added_at')
USER_LIST_COLS = ('id', 'user_id', 'name', 'name_en', 'color', 'is_default', 'is_system',
                  'project_id', 'deleted_at', 'created_at')
PROJECT_COLS = ('id', 'user_id', 'name', 'color', 'created_at')
DEFAULTS = {'list_items': {'tags': []}, 'user_lists': {'color': '#FFD700', 'is_default': False,
                                                       'is_system': False},
            'projects': {'color': '#4CAF50'}}
NOT_NULL = {'list_items': ('sys_id',), 'user_lists': ('name',), 'projects': ('name',)}


def _eq(a, b):
    """PostgREST casts the filter text to the column type: 101 and '101' are equal; NULL equals nothing."""
    if a is None or b is None:
        return False
    return a == b or str(a) == str(b)


def _integer(val):
    """A filter value on an integer column (the SERIAL ids), as PostgREST casts it: text that is no
    integer is an error."""
    try:
        return int(val)
    except (TypeError, ValueError):
        raise APIError({'code': '22P02', 'message': f'invalid input syntax for type integer: "{val}"'})


def _jsonb_array(val):
    if not isinstance(val, str):
        # postgrest-py turns a Python list into the text[] literal {a,b}: invalid JSON for jsonb
        raise APIError({'code': '22P02', 'message': 'invalid input syntax for type json'})
    try:
        parsed = json.loads(val)
    except ValueError:
        raise APIError({'code': '22P02', 'message': 'invalid input syntax for type json'})
    if not isinstance(parsed, list):
        raise APIError({'code': '22P02', 'message': 'expected a json array'})
    return parsed


def _json_set(values):
    return {json.dumps(x, ensure_ascii=False, sort_keys=True) for x in values}


def match_row(row, filters):
    for kind, col, val in filters:
        v = row.get(col)
        if kind == 'eq':
            if not _eq(v, val):
                return False
        elif kind == 'in':
            if not any(_eq(v, x) for x in val):
                return False
        elif kind == 'is':
            if not (str(val).lower() == 'null' and v is None):
                return False
        elif kind == 'gt':
            limit = _integer(val)
            if v is None or not _integer(v) > limit:
                return False
        elif kind in ('cs', 'cd'):
            want = _json_set(_jsonb_array(val))
            if v is None:
                return False
            have = _json_set(v)
            if kind == 'cs' and not want <= have:
                return False
            if kind == 'cd' and not have <= want:
                return False
        else:
            raise AssertionError(kind)
    return True


def keyset_after(req):
    """The id a keyset page starts after (its `gt` filter on id), or None."""
    return next((val for kind, col, val in req.filters if kind == 'gt' and col == 'id'), None)


def is_later_page(req):
    """A list_items select that continues a paged read: an offset past 0 (the pre-2b and
    offset engines) or a page after a row id (keyset)."""
    return (req.t == 'list_items' and req.op == 'select'
            and (bool(req.rng and req.rng[0] > 0) or keyset_after(req) is not None))


def query_string_length(filters):
    """Bytes of the URL query string PostgREST would receive for these filters."""
    parts = []
    for kind, col, val in filters:
        if kind == 'in':
            text = '(' + ','.join(str(x) for x in val) + ')'
        else:
            text = val if isinstance(val, str) else str(val)
        parts.append(f'{col}={kind}.{urllib.parse.quote(text, safe="")}')
    return len('&'.join(parts))


class FakeDB:
    def __init__(self, rng, has_page, max_rows, past_end_raises, page_lag):
        self.rng = rng
        self.has_page = has_page
        self.max_rows = max_rows
        self.past_end_raises = past_end_raises
        self.page_lag = page_lag
        self.page_lag_writes = 1 if (page_lag and has_page) else 0
        self.lagged = False  # a write failed on the lagging schema cache during this step
        self.gateway_limit = 8192
        self.tables = {'projects': [], 'user_lists': [], 'list_items': []}
        self._next = 100
        self.clock = 0
        self.world = None

    def new_id(self):
        self._next += 1
        return self._next

    def cols(self, table):
        if table == 'list_items':
            return LIST_ITEM_COLS + (('page',) if self.has_page else ())
        return USER_LIST_COLS if table == 'user_lists' else PROJECT_COLS

    def owner(self, table, row):
        if table == 'list_items':
            lst = self.list_by_id(row.get('list_id'))
            return lst['user_id'] if lst else None
        return row.get('user_id')

    def list_by_id(self, list_id):
        for lst in self.tables['user_lists']:
            if _eq(lst['id'], list_id):
                return lst
        return None

    def migrate(self):
        if self.has_page:
            return False
        self.has_page = True
        for r in self.tables['list_items']:
            r.setdefault('page', None)
        if self.page_lag:
            self.page_lag_writes = 1
        return True

    def visible(self, table, user):
        if user is None:
            return []
        return [r for r in self.tables[table] if self.owner(table, r) == user]

    def matching(self, table, filters, user):
        for kind, _, val in filters:
            if kind == 'gt':
                _integer(val)   # PostgREST rejects the value before it looks at a row
        return [r for r in self.visible(table, user) if match_row(r, filters)]


class _Auth:
    def __init__(self, client):
        self.client = client

    def get_session(self):
        if self.client.session_user is None:
            return None
        return types.SimpleNamespace(user=types.SimpleNamespace(id=self.client.session_user))


class FakeClient:
    """One supabase-py client: a desktop or the website, with its own session."""

    def __init__(self, db, actor, user):
        self.db, self.actor, self.session_user = db, actor, user
        self.auth = _Auth(self)
        self.url_limit = None
        self.page_missing = False

    def table(self, name):
        return _Req(self, name)


class _Req:
    def __init__(self, client, table):
        self.c, self.t = client, table
        self.op, self.cols_sel, self.payload = 'select', '*', None
        self.filters = []
        self.count = None
        self.order_by = None
        self.rng = None
        self.limit_n = None
        self.result = self.answer = None

    def select(self, cols='*', count=None, **kw):
        self.op, self.cols_sel, self.count = 'select', cols, count
        return self

    def insert(self, payload, **kw):
        self.op, self.payload = 'insert', copy.deepcopy(payload)
        return self

    def update(self, payload, **kw):
        self.op, self.payload = 'update', copy.deepcopy(payload)
        return self

    def delete(self, **kw):
        self.op = 'delete'
        return self

    def eq(self, col, val):
        self.filters.append(('eq', col, val))
        return self

    def in_(self, col, vals):
        self.filters.append(('in', col, list(vals)))
        return self

    def is_(self, col, val):
        self.filters.append(('is', col, 'null' if val is None else val))
        return self

    def gt(self, col, val):
        self.filters.append(('gt', col, val))
        return self

    def contains(self, col, val):
        self.filters.append(('cs', col, val))
        return self

    def contained_by(self, col, val):
        self.filters.append(('cd', col, val))
        return self

    def order(self, col, desc=False, **kw):
        self.order_by = (col, desc)
        return self

    def range(self, start, end):
        self.rng = (start, end)
        return self

    def limit(self, n):
        self.limit_n = n
        return self

    # -- the request as the world sees it
    def selected_cols(self):
        if self.op != 'select' or self.cols_sel.strip() == '*':
            return ()
        return tuple(c.strip() for c in self.cols_sel.split(','))

    def payloads(self):
        if self.payload is None:
            return []
        return list(self.payload) if isinstance(self.payload, list) else [self.payload]

    def execute(self):
        db, c = self.c.db, self.c
        world = db.world
        action = world.before(c, self) if world is not None else None
        user = None if action == 'anon' else c.session_user
        if action == 'raise_before':
            raise httpx.ReadTimeout('injected: the request never reached the database')
        if action == 'offline':
            raise httpx.ConnectError('injected: no network')
        if action == 'gone_before':
            raise _ProcessGone('the program went away before this request was sent')
        if action == 'api_23502':
            raise APIError({'code': '23502', 'message': 'null value in column violates not-null constraint'})
        if action == 'api_pgrst103':   # (directed tests only) a range error on a read that asked for none
            raise APIError({'code': 'PGRST103', 'message': 'Requested range not satisfiable'})
        if self.op in ('update', 'delete'):
            limit = db.gateway_limit if c.url_limit is None else min(db.gateway_limit, c.url_limit)
            if query_string_length(self.filters) > limit:
                raise APIError({'code': 414, 'message': '<html>414 Request-URI Too Large</html>'})
        out = self._run(user)
        self.result = out                                    # what the database did
        if action == 'no_rows':                              # written, but the answer shows no row
            out = types.SimpleNamespace(data=[], count=out.count)
        self.answer = None if action in UNANSWERED else out   # what the engine will be given
        if world is not None:
            world.after(c, self)
        if action == 'gone_after':
            raise _ProcessGone('the program went away before the answer came back')
        if action == 'raise_after':
            raise httpx.ReadTimeout('injected: the response was lost after the database applied it')
        if action == 'api_pgrst111':
            raise APIError({'code': 'PGRST111', 'message': 'response headers could not be set'})
        if action == 'api_504':
            raise APIError({'code': 504, 'message': '<html>504 Gateway Time-out</html>'})
        return out

    def _missing(self, name, write):
        if name == 'page':
            self.c.page_missing = True
        if write:
            raise APIError({'code': 'PGRST204', 'message':
                            f"Could not find the '{name}' column of '{self.t}' in the schema cache"})
        raise APIError({'code': '42703', 'message': f'column {self.t}.{name} does not exist'})

    def _check_write_cols(self, names):
        allowed = self.c.db.cols(self.t)
        for n in names:
            if n not in allowed:
                self._missing(n, write=True)
        db = self.c.db
        if self.t == 'list_items' and 'page' in names and db.page_lag_writes > 0:
            db.page_lag_writes -= 1
            db.lagged = True
            self._missing('page', write=True)

    def _run(self, user):
        db = self.c.db
        rows = db.tables[self.t]
        if self.op == 'select':
            names = self.selected_cols()
            for n in names:
                if n not in db.cols(self.t):
                    self._missing(n, write=False)
            match = db.matching(self.t, self.filters, user)
            total = len(match)
            if self.order_by:
                col, desc = self.order_by
                match = sorted(match, key=lambda r: (r.get(col) is None, r.get(col) or 0), reverse=desc)
            else:
                match = list(match)
                db.rng.shuffle(match)  # PostgreSQL guarantees no order without ORDER BY
            if self.rng:
                start, end = self.rng
                if start > 0 and start >= total and db.past_end_raises:
                    raise APIError({'code': 'PGRST103', 'message': 'Requested range not satisfiable'})
                match = match[start:end + 1]
            if self.limit_n is not None:
                match = match[:self.limit_n]
            if db.max_rows is not None:
                match = match[:db.max_rows]
            data = [({k: v for k, v in r.items() if not k.startswith('_')} if not names
                     else {n: r.get(n) for n in names}) for r in match]
            return types.SimpleNamespace(data=copy.deepcopy(data), count=total if self.count else None)
        if self.op == 'insert':
            payloads = self.payloads()
            for p in payloads:
                self._check_write_cols(list(p.keys()))
            new = []
            for p in payloads:
                row = {c: None for c in db.cols(self.t)}
                row.update(copy.deepcopy(DEFAULTS.get(self.t, {})))
                row.update(copy.deepcopy(p))
                for col in NOT_NULL.get(self.t, ()):
                    if row.get(col) is None:
                        raise APIError({'code': '23502', 'message': f'null value in column "{col}"'})
                if user is None or db.owner(self.t, row) != user:
                    raise APIError({'code': '42501', 'message': 'new row violates row-level security policy'})
                db.clock += 1
                row['id'] = db.new_id()
                if self.t == 'list_items':
                    row['added_at'] = db.clock
                    # had_page: the column existed and this client had not found it missing
                    row['_ghost'] = (self.c.actor, row.get('sys_id'), norm(row.get('fl_id')),
                                     norm(row.get('page')), db.has_page and not self.c.page_missing)
                new.append(row)
            rows.extend(new)
            out = [{k: v for k, v in r.items() if not k.startswith('_')} for r in new]
            db.rng.shuffle(out)  # a bulk insert's response order is not the payload's
            return types.SimpleNamespace(data=copy.deepcopy(out), count=None)
        if self.op == 'update':
            self._check_write_cols(list(self.payload.keys()))
            match = db.matching(self.t, self.filters, user)
            for r in match:
                r.update(copy.deepcopy(self.payload))
            out = [{k: v for k, v in r.items() if not k.startswith('_')} for r in match]
            return types.SimpleNamespace(data=copy.deepcopy(out), count=None)
        if self.op == 'delete':
            match = db.matching(self.t, self.filters, user)
            for r in match:
                rows.remove(r)
            if self.t == 'user_lists':  # ON DELETE CASCADE
                gone = {r['id'] for r in match}
                db.tables['list_items'][:] = [r for r in db.tables['list_items'] if r['list_id'] not in gone]
            if self.t == 'projects':    # ON DELETE SET NULL
                gone = {r['id'] for r in match}
                for lst in db.tables['user_lists']:
                    if lst.get('project_id') in gone:
                        lst['project_id'] = None
            out = [{k: v for k, v in r.items() if not k.startswith('_')} for r in match]
            return types.SimpleNamespace(data=copy.deepcopy(out), count=None)
        raise AssertionError(self.op)


# --------------------------------------------------------------------------- desktops

class Violation(Exception):
    def __init__(self, inv, kind, msg):
        super().__init__(f'[inv {inv}] {msg}')
        self.inv, self.kind, self.msg = inv, kind, msg

    @property
    def sig(self):
        return (self.inv, self.kind)


class Desk:
    """One computer: a ListsManager whose saves and snapshots stay in memory, and its sync."""

    def __init__(self, world, name, engine):
        self.world, self.name = world, name
        self.snaps = []
        self.disk = None
        # the world's record of this desktop's pending renames as it stood at the last save
        # and at each snapshot, so a restart or a backup recovery brings back the record
        # that belongs to the lists.pkl it brings back
        self.disk_renames = {}
        self.snap_renames = []
        self.user = USER
        self.client = FakeClient(world.db, name, USER)
        self.lm = self._manager(None)
        self.engine = engine
        self.sync = self._sync(engine)
        self.reidentified = set()  # items whose fl_id a user op set or changed (add_item on an existing key)
        self._runner_state()

    def _runner_state(self):
        self.copy = None           # the store the running upload works on (the runner's copy), else None
        self.upload_id = None      # the harness's number of the upload running on this desktop
        self.reports = None        # what that upload reported (on_recorded)
        self.signed_out = False
        self.offline = 0           # passes left in which every request of this desktop fails
        self.offline_now = False
        self.gone = None           # {'at', 'count', 'applied', 'how'}: the program goes away at that request
        self.ui = dict(UI_DEFAULT)
        self.snap_ledger = []      # (len(world.ledger), this desktop's move map) when each snapshot was taken

    @property
    def pass_store(self):
        """The store the pass in progress reads and writes: the runner's copy during an upload."""
        return self.copy if self.copy is not None else self.lm.data

    def copy_path(self):
        return hasattr(self.lm, 'begin_upload') and _takes(self.sync.sync_to_cloud, 'data')

    def fetch_path(self):
        return hasattr(self.lm, 'remembered_row_ids') and hasattr(self.sync, 'fetch_cloud_state')

    def _manager(self, data):
        desk = self

        class ScenarioListsManager(lm_mod.ListsManager):
            LISTS_FILE = os.path.join(_NOWHERE, f'{desk.name}.pkl')

            def save(self):
                desk.disk = pickle.dumps(self.data)  # also proves the store stays picklable
                desk.disk_renames = desk.renames()
                return True

            def write_snapshot(self, label, payload=None):
                desk.snaps.append(copy.deepcopy(self.data) if payload is None else pickle.loads(payload))
                snap_renames = desk.__dict__.setdefault('snap_renames', [])
                snap_renames.append(desk.renames())
                snap_ledger = desk.__dict__.setdefault('snap_ledger', [])
                snap_ledger.append((len(getattr(desk.world, 'ledger', ())),
                                    {k: v for k, v in getattr(desk.world, 'moves', {}).items() if k[0] == desk.name}))
                del desk.snaps[:-12]
                del snap_renames[:-12]
                del snap_ledger[:-12]
                return True

        lm = ScenarioListsManager(None)
        if data is not None:
            lm.data = data
        return lm

    def renames(self):
        """A copy of the world's record of this desktop's pending renames (none for a stand-in world)."""
        of = getattr(self.world, 'renames_of', None)
        return of(self) if of is not None else {}

    def _sync(self, engine):
        s = engine.ListsCloudSync(self.lm)
        s.set_client(self.client)
        s.set_user(self.user)
        return s

    def use_engine(self, engine):
        self.engine = engine
        self.sync = self._sync(engine)

    def clone(self, world):
        """This desktop in another world: its stores copied, its client and sync made anew."""
        n = Desk.__new__(Desk)
        skip = ('world', 'client', 'lm', 'engine', 'sync', 'copy', 'reports')
        n.__dict__.update({k: copy.deepcopy(v) for k, v in self.__dict__.items() if k not in skip})
        n.world, n.engine, n.copy, n.reports = world, self.engine, None, None
        n.client = FakeClient(world.db, self.name, self.client.session_user)
        n.client.page_missing, n.client.url_limit = self.client.page_missing, self.client.url_limit
        n.lm = n._manager(copy.deepcopy(self.lm.data))
        n.sync = n._sync(self.engine)
        if self.signed_out:
            n.sync.clear_user()
        return n

    def restart(self):
        data = pickle.loads(self.disk) if self.disk is not None else None
        self.world.set_renames(self, self.disk_renames)
        self.lm = self._manager(data)
        self.sync = self._sync(self.engine)
        self.copy = self.reports = self.upload_id = self.gone = None
        self.offline_now = False
        if self.signed_out:
            self.sync.clear_user()   # the saved sign-in was removed at sign-out

    def sign_in(self, user):
        self.user = user
        self.signed_out = False
        self.client.session_user = user
        self.sync.set_client(self.client)   # a sign-out may have dropped the client
        self.sync.set_user(user)

    def sign_out(self):
        """The sign-out's end: list sync off (ListsManager.disable_cloud_sync, on this desktop's own sync)."""
        self.sync.clear_user()
        self.signed_out = True

    @property
    def data(self):
        return self.lm.data


def _records(it):
    return [(k, r) for k, r in (it.get('cloud_rows') or {}).items() if isinstance(r, dict)]


class _Step:
    """What the world needs to know about the sync step in progress."""

    def __init__(self, desk, inject):
        self.desk = desk
        self.inject = inject
        self.req_count = 0
        self.fired = False
        self.counting = False
        self.web_deleted = set()
        self.same_edit_removed = set()
        self.same_edit_fired = False
        self.anon_lists = set()  # lists whose read an injected anonymous request answered (empty, count 0)
        self.phase = None        # the direction of the pass in progress: 'up' or 'down'
        self.armed = False       # web_churn_same_count added its row before a read
        self.start_rows = {}
        self.pass_start_lists = {}
        self.pass_start_items = {}
        self.desk_deleted = set()     # rows a desktop's DELETE removed in this step
        self.edit_in_upload = False   # the same-desktop edit ran while an upload worked on its copy
        self.twin_excluded = False    # ... in an upload whose kept deletes may delete a live row (invariant 14)
        self.after_insert = False     # the step's desktop had an insert answered
        self.after_move = False       # ... or a move of a row to another list
        self.defer_edit = False       # the twin world: hold the same-desktop edit until the upload is installed
        self.deferred = None


class World:
    def __init__(self, seed, cfg, engine, fixture=None, checks=ALL_CHECKS):
        self.seed, self.cfg = seed, cfg
        self.checks = set(checks)
        self.engine = engine
        self.fixture = fixture
        self.db = FakeDB(random.Random(seed ^ 0x5EED), has_page=cfg['has_page'], max_rows=cfg['max_rows'],
                         past_end_raises=cfg.get('past_end_raises', False), page_lag=cfg.get('page_lag', False))
        self.db.world = self
        self.web = FakeClient(self.db, 'web', USER)
        upgrade = cfg.get('upgrade', 0) and fixture is not None and fixture is not engine
        self.switch_at = cfg.get('upgrade', 0) if upgrade else 0
        first = fixture if upgrade else engine
        self.checking = not upgrade
        self.legacy_claims = first is fixture or engine is fixture
        # (desk, list id) -> the name a desktop rename gave it, while it is pending (invariant 10)
        self.renamed = {}
        # desk -> {cloud list id: name} of list inserts answered with the row: the pass gives
        # the list its id only after the answer, so the record is settled at the next save
        self.named_inserts = {}
        self.desks = {'A': Desk(self, 'A', first), 'B': Desk(self, 'B', first)}
        self.tok = 0
        self.live_n, self.live_g = set(), set()
        # retired note tokens a paste made live again, by the row they were pasted into: if that
        # row goes, or a desktop's edit that retired them reaches it (a note write lands only on
        # the note the desktop last saw, so the paste had put that back), they are retired again
        # (the older copies were already being replaced)
        self.revived = {}
        # invariant 11: one entry per explicit removal a desktop made (see _ledger_after)
        self.ledger = []
        # the harness's own map of moves: (desk, row id) -> (item id, local list it is bound for)
        self.moves = {}
        # every insert of a list_items row and every change of a row's list, by a desktop (_log_rows)
        self.row_log = []
        self.killed = set()      # uploads whose process was killed
        self.user_ops = 0        # user ops the ledger has seen (a removal's entries share its number)
        self.upload_moves = []   # moves made while an upload ran: (desk, upload, item, identity, from, to)
        self.explain_only = {}   # (desk, item) -> rows a removal's DELETE may take that it does not expect
        self.forgot = {}         # desk -> length of the request log when a backup recovery made it forget
        self.upload_seq = 0
        self.cur_step = None
        self.twins = 0           # steps invariant 14 replayed
        self.ctx = None
        self.pending = []
        self.trace = []
        self._seed_the_cloud()

    # ---- setup
    def _seed_the_cloud(self):
        """A website list holding a My Library row that an older desktop uploaded."""
        lst = self.web.table('user_lists').insert({'user_id': USER, 'name': NAMES[0], 'name_en': NAMES[0],
                                                   'color': '#FFD700', 'is_default': False,
                                                   'is_system': False}).execute().data[0]
        self.web.table('list_items').insert({'list_id': lst['id'], 'sys_id': SEEDED_LOCAL_SYS, 'shelfmark': None,
                                             'title': None, 'fl_id': None, 'note': '', 'tags': []}).execute()

    def switch_engine(self):
        for d in self.desks.values():
            d.use_engine(self.engine)
        self.checking = True
        self.renamed, self.named_inserts = {}, {}
        self.ledger, self.moves, self.row_log = [], {}, []
        for d in self.desks.values():
            d.disk_renames, d.snap_renames = {}, [{} for _ in d.snaps]
            d.snap_ledger = [(0, {}) for _ in d.snaps]
        self.legacy_claims = self.engine is self.fixture
        self.live_n, self.live_g = self.copies()
        # the rows as the old engine left them are the starting point of the identity checks
        for r in self.db.tables['list_items']:
            g = r.get('_ghost')
            if g:
                r['_ghost'] = (g[0], r.get('sys_id'), norm(r.get('fl_id')), norm(r.get('page')), g[4])
        self.trace.append(f'-- the engine under test takes over; {len(self.live_n)} note and '
                          f'{len(self.live_g)} tag tokens are live')

    # ---- violations
    def pend(self, inv, kind, msg):
        if self.checking and inv in self.checks:
            self.pending.append(Violation(inv, kind, msg))

    def viol(self, inv, kind, msg):
        if self.checking and inv in self.checks:
            raise Violation(inv, kind, msg)

    def flush(self, where):
        if self.pending:
            v = self.pending[0]
            self.pending = []
            raise Violation(v.inv, v.kind, f'{where}: {v.msg}')

    # ---- tokens
    def fresh(self, p):
        self.tok += 1
        return f'{p}{self.tok}'

    def copies(self):
        notes, tags = set(), set()
        for d in self.desks.values():
            for it in d.data.get('items', {}).values():
                notes |= toks(it.get('note'))
                tags |= tag_toks(it.get('tags'))
        for r in self.db.tables['list_items']:
            notes |= toks(r.get('note'))
            tags |= tag_toks(r.get('tags'))
        return notes, tags

    def unrevive(self, row_ids):
        for t, rid in list(self.revived.items()):
            if rid in row_ids:
                self.live_n.discard(t)
                del self.revived[t]

    def retire_if_lost(self):
        """After a removal: what is now in no copy was removed by the user (2b-1 propagates no removal)."""
        n, g = self.copies()
        self.live_n &= n
        self.live_g &= g

    def check_tokens(self, where):
        n, g = self.copies()
        lost_n, lost_g = self.live_n - n, self.live_g - g
        if lost_n or lost_g:
            self.viol(1, 'text-lost', f'{where}: user text lost from every copy: notes {sorted(lost_n)} '
                                      f'tags {sorted(lost_g)}')

    # ---- the request hook
    def before(self, client, req):
        if client.actor not in self.desks:
            return None
        d = self.desks[client.actor]
        if self.checking:
            self._check_request(d, client, req)
        if d.offline_now:
            return 'offline'
        g = d.gone
        if g is not None and client is d.client:
            g['count'] += 1
            if g['at'] == 'write' and g.get('wrote') or g['at'] != 'write' and g['count'] >= g['at']:
                d.gone = None
                if g.get('edit') is not None and d.copy is not None:
                    self.trace.append(f'    {d.name}: a user edit, then the program goes away')
                    self._same_desk_edit(d, g['edit'])
                self.trace.append(f'    {d.name}: the program goes away at request {g["count"]} ({req.op} {req.t}), '
                                  + ('after' if g['applied'] else 'before') + ' it reached the database')
                return 'gone_after' if g['applied'] else 'gone_before'
        c = self.ctx
        if (c is not None and c.inject and c.counting and not c.fired and client is c.desk.client):
            c.req_count += 1
            if c.inject[1] == 'web_between_pages':
                # the first later page of any list's read in the step
                if is_later_page(req):
                    c.fired = True
                    return self._fire(c, client, req)
            elif c.inject[1] == 'web_churn_same_count':
                # An upload's reads only (in a Merge the Download would pair the row and the upload
                # then insert nothing). Just before an upload reads a list, the website may add a
                # row for an entry this desktop holds there with no record; then, at the first
                # later page from which the churn can take the read past such a row, it fires.
                if c.phase == 'up' and req.t == 'list_items' and req.op == 'select':
                    if not is_later_page(req):
                        self._churn_arm(c, client, req)
                    elif self._between_pages(c, client, req)[3] is not None:
                        c.fired = True
                        return self._fire(c, client, req)
            elif c.inject[1] in ('raise_after', 'api_error') and c.inject[2][1] % 3 == 0:
                # a third of these hit the step's first batch insert: it commits, then the answer fails
                if req.t == 'list_items' and req.op == 'insert' and isinstance(req.payload, list):
                    c.fired = True
                    return self._fire(c, client, req)
            elif c.inject[1] in ('edit_after_insert', 'edit_after_move'):
                # the same-desktop edit, right after an insert (or a move) of the step's desktop was answered
                if c.after_insert if c.inject[1] == 'edit_after_insert' else c.after_move:
                    c.fired = True
                    return self._fire(c, client, req)
            elif c.req_count == c.inject[0]:
                c.fired = True
                return self._fire(c, client, req)
        return None

    def after(self, client, req):
        if client.actor in self.desks and req.t == 'user_lists' and req.op in ('update', 'insert') \
                and 'name' in (req.payload or {}):
            self._rename_written(self.desks[client.actor], req)
        if client.actor in self.desks and req.t == 'list_items' and req.op == 'update' \
                and 'note' in (req.payload or {}):
            self._revived_row_written(req)
        served = getattr(self, 'served', None)
        if (served is not None and client is served[0].client and req.t == 'list_items' and req.op == 'select'
                and req.answer is not None):
            for r in req.answer.data or []:
                if r.get('id') is not None:
                    served[1][r['id']] = dict(r)
        if client.actor in self.desks and req.t == 'list_items':
            d = self.desks[client.actor]
            if req.op == 'insert' and self.ctx is not None and client is self.ctx.desk.client                     and req.answer is not None:
                self.ctx.after_insert = True
            if req.op == 'update' and 'list_id' in (req.payload or {}) and self.ctx is not None                     and client is self.ctx.desk.client and req.answer is not None and req.answer.data:
                self.ctx.after_move = True
            wrote = req.op == 'insert' or (req.op == 'update' and 'list_id' in (req.payload or {}))
            if wrote and d.gone is not None and client is d.client and req.answer is not None:
                d.gone['wrote'] = True
            if req.op == 'insert' or (req.op == 'update' and 'list_id' in (req.payload or {})):
                self._log_rows(d, req)
            elif req.op == 'delete':
                self._deleted(d, req)
        return None

    def _log_rows(self, d, req):
        """The request log of invariant 11: rows a desktop inserted, or moved to another list."""
        store = d.pass_store
        rows = (req.result.data or []) if req.result is not None else []
        target = req.payloads()[0].get('list_id') if req.op == 'insert' else req.payload['list_id']
        local = next((lid for lid, ld in (store.get('lists') or {}).items() if _eq(ld.get('cloud_id'), target)), None)
        for r in rows:
            e = row_ident(r, 'page' in r and norm(r.get('page')) is not None)
            twins = sum(1 for iid, it in (store.get('items') or {}).items() if _ident_matches(ident(iid, it), e))
            self.row_log.append({'n': len(self.row_log), 'desk': d.name, 'upload': d.upload_id, 'step': self.cur_step,
                                 'rid': r['id'],
                                 'target': r.get('list_id', target), 'ident': e, 'kind': req.op,
                                 'answered': req.answer is not None, 'local': local, 'ambiguous': twins > 1})

    def _deleted(self, d, req):
        rows = (req.result.data or []) if req.result is not None else []
        gone = {r['id'] for r in rows}
        if not gone:
            return
        if self.ctx is not None:
            self.ctx.desk_deleted |= gone
        self.unrevive(gone)
        explicit = {col for kind, col, _ in req.filters} <= {'id', 'list_id'}
        self.trace.append(f'    {d.name}: {"explicit" if explicit else "conditional"} DELETE of row(s) {sorted(gone)}')
        if explicit:
            # what the user removed goes with the row (a conditional delete takes only text its entry holds)
            self.retire_if_lost()
        elif d.copy is not None:
            # the upload's copy of the entry held that text; if the user removed it from the entry (or the
            # entry) meanwhile, that removal counts as made after the upload: its text goes with it
            live = d.data.get('items') or {}
            copies = [iid for iid, it in d.copy.get('items', {}).items() for k, r in _records(it)
                      if str(k).startswith('~') and str(r.get('id')) in {str(g) for g in gone}]
            if any(iid not in live or toks(live[iid].get('note')) != toks(d.copy['items'][iid].get('note'))
                   or tag_toks(live[iid].get('tags')) != tag_toks(d.copy['items'][iid].get('tags'))
                   for iid in copies):
                self.retire_if_lost()

    def _revived_row_written(self, req):
        rid = next((val for kind, col, val in req.filters if kind == 'eq' and col == 'id'), None)
        row = next((r for r in self.db.tables['list_items'] if _eq(r['id'], rid)), None) if rid is not None else None
        if row is None or (row.get('note') or '') != (req.payload['note'] or ''):
            return   # the write did not land
        for t, r in list(self.revived.items()):
            if _eq(r, rid) and t not in toks(row.get('note')):
                self.live_n.discard(t)
                del self.revived[t]
                self.trace.append(f'    {t}, pasted onto row {rid}, is replaced there by the edit that retired it')

    def _rename_written(self, d, req):
        """A pending rename is sent once a write of that name to the list's cloud list was
        answered with the row (an anonymous write returns none; a lost answer is no answer)."""
        rows = (req.answer.data or []) if req.answer is not None else []
        new = req.payload['name']
        for cl in rows:
            if cl.get('name') != new:
                continue
            if req.op == 'insert':
                self.named_inserts.setdefault(d.name, {})[cl['id']] = new
                continue
            for key, nm in list(self.renamed.items()):
                ld = d.pass_store['lists'].get(key[1]) if key[0] == d.name else None
                if ld is not None and nm == new and _eq(ld.get('cloud_id'), cl['id']):
                    del self.renamed[key]

    def settle_inserts(self, d):
        """The pending renames an answered insert of the list's cloud list has sent."""
        named = self.named_inserts.pop(d.name, {})
        if not named or d.data is None:
            return
        for key, nm in list(self.renamed.items()):
            ld = d.data['lists'].get(key[1]) if key[0] == d.name else None
            if ld is not None and any(_eq(ld.get('cloud_id'), cid) and nm == new for cid, new in named.items()):
                del self.renamed[key]

    def renames_of(self, d):
        """A copy of this desktop's pending renames, {list id: name} (settling answered inserts first)."""
        if not hasattr(self, 'renamed'):
            return {}
        self.settle_inserts(d)
        return {lid: nm for (dn, lid), nm in self.renamed.items() if dn == d.name}

    def set_renames(self, d, record):
        """This desktop's pending renames become `record` (its lists.pkl came back from a copy)."""
        self.named_inserts.pop(d.name, None)
        for key in [k for k in self.renamed if k[0] == d.name]:
            del self.renamed[key]
        for lid, nm in record.items():
            self.renamed[(d.name, lid)] = nm

    def _fire(self, c, client, req):
        kind, sel = c.inject[1], c.inject[2]
        self.trace.append(f'    injected {kind} at request {c.req_count} ({req.op} {req.t})')
        if kind == 'anon' and req.t == 'list_items' and req.op == 'select':
            for k, col, val in req.filters:
                if k == 'eq' and col == 'list_id':
                    c.anon_lists.add(val)
        if kind in ('raise_before', 'raise_after', 'anon'):
            return kind
        if kind == 'api_error':
            return ('api_23502', 'api_pgrst111', 'api_504')[sel[0] % 3]
        if kind == 'session_lost':
            client.session_user = None
            return None
        if kind == 'url_too_long':
            client.url_limit = 32  # a note or tag filter no longer fits the gateway's URL limit
            return None
        if kind == 'web':
            self.web_op(sel)
            return None
        if kind == 'web_rename':
            self.web_rename(sel)
            return None
        if kind in ('web_between_pages', 'web_churn_same_count'):
            # The website removes rows the earlier pages returned. A read paged by offset then
            # starts its next page later and skips rows it never returned: preferably up to a
            # row this desktop holds the entry of with no record (the one an insert would
            # duplicate). web_between_pages leaves the count lower, which such a read can
            # notice; web_churn_same_count also adds one row per row removed (higher ids),
            # so the count and the number of ids the read collects both come out right while
            # the skipped rows were never returned. A read paged by keyset on the row id
            # skips nothing either way.
            lid, rows, start, reach = self._between_pages(c, client, req)
            lst = self.db.list_by_id(lid) if lid is not None else None
            if lst is None or lst.get('user_id') != USER:
                return None   # the website is USER's (web_lists): another account's list is not its to change
            t = reach if reach is not None else next(
                (t for t in range(start, len(rows)) if self._unrecorded(c, client, lid, rows[t])), start)
            gone = rows[:max(1, min(t - start + 1, start))] if start else []
            for r in gone:
                self.web_remove(r)
            if kind == 'web_churn_same_count':
                for n in range(len(gone)):
                    self.web_add(lst, sel[n:] + sel[:n])
            return None
        if kind == 'other_desktop_pass':
            other = self.desks['B' if c.desk.name == 'A' else 'A']
            direction = 'up' if sel[0] % 2 else 'down'
            if other.signed_out:
                return None
            other.client.page_missing = False
            res = self.upload(other) if direction == 'up' else self.download(other)
            self.trace.append(f'    {other.name}: {direction} (while {c.desk.name} syncs) -> {_summary(res)}')
            return None
        if kind in ('same_desktop_edit', 'edit_after_insert', 'edit_after_move'):
            d = c.desk
            # an answer to the website-removal prompt, for the entries it would list now
            asked = self.pending_web_removals(d) if sel != LAST_WRITE and (sel[0] // 12) % 6 == 0 else []
            if c.defer_edit and d.copy is not None:
                # the twin world: made right after this upload is installed, as it was made here
                c.deferred = (sel, asked, self.pin(d, sel))
                return None
            c.edit_in_upload = c.edit_in_upload or d.copy is not None
            self._same_desk_edit(d, sel, asked)
            return None
        raise AssertionError(kind)

    def _between_pages(self, c, client, req):
        """(list id, the list's rows by id, how many of them the earlier pages covered, reach):
        reach is the first row a removal of rows already returned can take an offset read past
        (at most as many rows as were returned) that this desktop holds with no record, or None."""
        lid = next((val for k, col, val in req.filters if k == 'eq' and col == 'list_id'), None)
        rows = sorted((r for r in self.db.tables['list_items'] if lid is not None and _eq(r['list_id'], lid)),
                      key=lambda r: r['id'])
        after = keyset_after(req)
        start = req.rng[0] if after is None else sum(1 for r in rows if r['id'] <= _integer(after))
        reach = next((t for t in range(start, min(len(rows), 2 * start)) if self._unrecorded(c, client, lid, rows[t])),
                     None)
        return lid, rows, start, reach

    def _churn_arm(self, c, client, req):
        """(web_churn_same_count, once a step) Just before an upload reads a list past the server's
        row cap: a row appears for an entry this desktop holds in that list with no record and no
        row there -- the same entry added in two places before either synced: on the website, or,
        with a page, on the other desktop. The row is there for the whole read, beyond its first
        page, so the upload must pair it, not insert."""
        lid = next((val for k, col, val in req.filters if k == 'eq' and col == 'list_id'), None)
        if c.armed or lid is None or self.db.max_rows is None:
            return
        data = c.desk.pass_store   # what the upload pairs against: the runner's copy while one runs
        local = next((k for k, ld in data['lists'].items() if _eq(ld.get('cloud_id'), lid)), None)
        rows = [r for r in self.db.tables['list_items'] if _eq(r['list_id'], lid)]
        lst = self.db.list_by_id(lid)
        if local is None or lst is None or lst.get('user_id') != USER or len(rows) < self.db.max_rows:
            return   # (the website is USER's: it adds no row to another account's list)
        hp = self.db.has_page and not client.page_missing
        wanted = []
        for iid, it in data['items'].items():
            e = ident(iid, it)
            if (local in (it.get('lists') or []) and not (it.get('cloud_rows') or {}).get(local)
                    and not e[0].startswith('97') and (e[2] is None or hp) and same_entry(e, e, hp)
                    and not any(same_entry(e, row_ident(r, hp), hp) for r in rows)):
                wanted.append(e)
        if not wanted:
            return
        c.armed = True
        sys_id, fl, page = sorted(wanted, key=repr)[c.inject[2][0] % len(wanted)]
        new = self.web.table('list_items').insert({'list_id': lid, 'sys_id': sys_id, 'shelfmark': web_shelfmark(sys_id),
                                                   'title': web_title(sys_id), 'fl_id': fl, 'note': '',
                                                   'tags': []}).execute().data[0]
        who = 'web'
        if page is not None:   # the website writes no page: the other desktop's upload of the entry
            who = 'B' if c.desk.name == 'A' else 'A'
            row = next(r for r in self.db.tables['list_items'] if r['id'] == new['id'])
            row['page'] = page   # (written in place: the lagging schema cache is the desktops' to meet)
            row['_ghost'] = (who, sys_id, fl, page, True)
        c.start_rows[new['id']] = lid   # there before the read began: a row the upload should claim
        self.trace.append(f'    {who}: add ({sys_id},{fl},{page}) to cloud list {lid} {lst["name"]} before '
                          f'{c.desk.name} reads it (row {new["id"]})')

    def _unrecorded(self, c, client, lid, r):
        """Row r is an entry this desktop holds in the list with no record, and no record names r."""
        hp = self.db.has_page and not client.page_missing
        data = c.desk.pass_store
        local = next((k for k, ld in data['lists'].items() if _eq(ld.get('cloud_id'), lid)), None)
        if local is None:
            return False
        named = {str(rec.get('id')) for it in data['items'].values() for _, rec in _records(it)}
        return str(r['id']) not in named and any(
            local in (it.get('lists') or []) and not (it.get('cloud_rows') or {}).get(local)
            and same_entry(ident(iid, it), row_ident(r, hp), hp) for iid, it in data['items'].items())

    def _same_desk_edit(self, d, sel, asked=(), pinned=None):
        c = self.ctx
        before = self.memberships(d)
        if asked:
            self.prompt(d, sel, during_pass=True, only=asked)
        elif sel == LAST_WRITE:
            self.remove_last_write(d, pinned['remove'] if pinned else self.last_write(d))
        else:
            self.desk_op(d, sel, during_pass=True, pinned=pinned)
        if c is not None:
            c.same_edit_removed |= before - self.memberships(d)
            c.same_edit_fired = True

    def last_write(self, d):
        """(item, list) of the entry whose row this desktop inserted or moved last, or None."""
        x = next((x for x in reversed(self.row_log) if x['desk'] == d.name and x['answered'] and x['local']), None)
        if x is None or x['local'] not in d.data['lists']:
            return None
        iid = next((i for i, it in sorted(d.data['items'].items()) if x['local'] in (it.get('lists') or [])
                    and _ident_matches(ident(i, it), x['ident'])), None)
        return None if iid is None else (iid, x['local'])

    def remove_last_write(self, d, target):
        """Remove, from its list, the entry whose row this desktop wrote last (the self-test mix)."""
        if target is None or target[0] not in d.data['items']:
            return
        snap = self._ledger_before(d)
        d.lm.remove_item_from_list(*target)
        self._ledger_after(d, snap, 3)
        self.trace.append(f'{d.name} (during its pass): remove {target[0]} from {target[1]} (its row was just written)')
        self.retire_if_lost()

    def _check_request(self, d, client, req):
        pls = req.payloads()
        name = d.name
        if req.op == 'delete':
            if req.t != 'list_items':
                self.pend(3, 'delete', f'{name} sent a DELETE on {req.t} {req.filters}')
            else:
                self._check_delete(d, client, req)
        for p in pls:
            if str(p.get('sys_id') or '').startswith('97'):
                self.pend(6, 'local-sent', f'{name} sent My Library sys_id {p.get("sys_id")} to {req.t}')
        for kind, col, val in req.filters:
            vals = val if isinstance(val, list) else [val]
            if col == 'sys_id' and any(str(v).startswith('97') for v in vals):
                self.pend(6, 'local-sent', f'{name} filtered {req.t} on My Library sys_id {val}')
        if req.t == 'user_lists' and req.op == 'update' and 'name' in (req.payload or {}):
            self._check_name_write(d, client, req)
        if req.t != 'list_items':
            return
        if client.page_missing and ('page' in req.selected_cols() or any('page' in p for p in pls)):
            self.pend('R', 'page-after-missing', f'{name} sent page after the column was found missing: '
                                                 f'{req.op} {req.selected_cols() or pls}')
        for p in pls:
            if 'page' in p and p['page'] in (None, ''):
                self.pend('R', 'page-none', f'{name} sent page={p["page"]!r} ({req.op})')
        if req.op == 'insert':
            for p in pls:
                self._check_insert(d, p)
        elif req.op == 'update':
            self._check_update(d, client, req)

    def _check_delete(self, d, client, req):
        """Invariants 3 and 7: a desktop's DELETE of a row is explicit or redundant, and nothing else."""
        flt = collections.defaultdict(list)
        for kind, col, val in req.filters:
            flt[(kind, col)].append(val)
        ids, lsts = flt.get(('eq', 'id'), []), flt.get(('eq', 'list_id'), [])
        if len(ids) != 1 or len(lsts) != 1:
            self.pend(3, 'delete-filter', f'{d.name} DELETE not filtered by one id and one list_id: {req.filters}')
            return
        rid, lst = ids[0], lsts[0]
        store = d.pass_store
        if set(flt) == {('eq', 'id'), ('eq', 'list_id')}:
            entry = (store.get('cloud_deletes') or {}).get(str(rid))
            if not isinstance(entry, dict):
                self.pend(3, 'delete-not-pending', f'{d.name} deleted row {rid} of cloud list {lst}, which is no '
                                                   f'pending removal of its store')
            elif entry.get('account') != d.user:
                self.pend(7, 'delete-other-account', f'{d.name} signed in as {d.user} deleted row {rid}, a pending '
                                                     f'removal of {entry.get("account")}')
            elif not _eq(entry.get('list'), lst):
                self.pend(3, 'delete-wrong-list', f'{d.name} deleted row {rid} filtered by cloud list {lst}, its '
                                                  f'removal names {entry.get("list")}')
            elif not self._explained(d, rid):
                self.pend(3, 'delete-unexplained', f'{d.name} deleted row {rid}: no removal of the ledger explains it')
            else:
                self._check_o12(d, rid)
            return
        # redundant: a moved row whose destination has its own row, deleted only as it is
        holder, moved = next(((it, r) for it in (store.get('items') or {}).values() for k, r in _records(it)
                              if str(k).startswith('~') and _eq(r.get('id'), rid)), (None, None))
        if holder is None:
            self.pend(3, 'delete-conditional-not-moved', f'{d.name} deleted row {rid} conditionally, but no moved '
                                                         f'row of its store has that id: {req.filters}')
            return
        if not (flt.get(('eq', 'note')) or flt.get(('is', 'note'))) or not (
                (flt.get(('cs', 'tags')) and flt.get(('cd', 'tags'))) or flt.get(('is', 'tags'))):
            self.pend(3, 'delete-conditional-filter', f'{d.name} deleted moved row {rid} without its note and tag '
                                                      f'filters: {req.filters}')
            return
        row = next((r for r in self.db.tables['list_items'] if _eq(r['id'], rid)), None)
        try:
            hits = row is not None and match_row(row, req.filters)
        except APIError:
            hits = False
        # the entry holds the text a user still has (invariant 1's live tokens) -- or the row is as
        # this desktop last agreed on it, and what its user changed since is the entry's own edit
        agreed = (moved.get('note') is not None and moved.get('tags') is not None
                  and ((row or {}).get('note') or '') == (moved.get('note') or '')
                  and _json_set((row or {}).get('tags') or []) == _json_set(moved.get('tags') or []))
        if hits and not agreed and not ((toks(row.get('note')) & self.live_n) <= toks(holder.get('note'))
                                        and (tag_toks(row.get('tags')) & self.live_g) <= tag_toks(holder.get('tags'))):
            self.pend(3, 'delete-text-not-held', f'{d.name} deleted moved row {rid} holding note '
                                                 f'{_short(row.get("note"))} tags {row.get("tags")}, which its entry '
                                                 f'({_short(holder.get("note"))}, {holder.get("tags")}) lacks')

    def _check_name_write(self, d, client, req):
        """An upload writes a list name over another only for a list renamed on that desktop."""
        try:
            rows = self.db.matching('user_lists', req.filters, client.session_user)
        except APIError:
            rows = []
        new = req.payload['name']
        for cl in rows:
            if cl['name'] == new:
                continue
            if not any(_eq(ld.get('cloud_id'), cl['id']) and ld.get('name') == new
                       and self.renamed.get((d.name, lid)) == new
                       for lid, ld in d.pass_store.get('lists', {}).items()):
                self.pend(10, 'name-written-back', f'{d.name} wrote the name {new!r} over {cl["name"]!r} on cloud '
                                                   f'list {cl["id"]}, which no list renamed on {d.name} holds')

    def _check_update(self, d, client, req):
        p = req.payload
        flt = {}
        for kind, col, val in req.filters:
            flt[(kind, col)] = val
        name = d.name
        if ('eq', 'id') not in flt or ('eq', 'list_id') not in flt:
            self.pend('R', 'update-filter', f'{name} update not filtered by id and list_id: {req.filters}')
        for col in ('sys_id', 'title'):
            if col in p:
                self.pend('R', 'update-field', f'{name} update writes {col}: {p}')
        for col in ('page', 'fl_id', 'shelfmark'):
            if col in p and p[col] in (None, ''):
                self.pend('R', 'update-clears', f'{name} update writes {col}={p[col]!r}')
        if 'note' in p and ('eq', 'note') not in flt and flt.get(('is', 'note')) != 'null':
            self.pend('R', 'note-unconditional', f'{name} writes a note without a note filter: {req.filters}')
        if 'tags' in p and flt.get(('is', 'tags')) != 'null':
            ok = True
            for k in ('cs', 'cd'):
                v = flt.get((k, 'tags'))
                try:
                    ok = ok and isinstance(v, str) and isinstance(json.loads(v), list)
                except ValueError:
                    ok = False
            if not ok:
                self.pend('R', 'tags-unconditional', f'{name} writes tags without JSON cs/cd filters: '
                                                     f'{req.filters}')
        try:
            rows = self.db.matching('list_items', req.filters, client.session_user)
        except APIError:
            rows = []
        if 'list_id' in p:
            self._check_move(d, req, rows)
        for r in rows:
            self._check_fill(d, r, p)

    def _names_row(self, it, rid, include_gone=False):
        for _, rec in _records(it):
            if _eq(rec.get('id'), rid) and (include_gone or not rec.get('gone')):
                return True
        return self.legacy_claims and it.get('cloud_id') is not None and _eq(it.get('cloud_id'), rid)

    def _check_move(self, d, req, rows):
        new = req.payload['list_id']
        src = None
        for kind, col, val in req.filters:
            if kind == 'eq' and col == 'list_id':
                src = val
        if src is None or _eq(src, new):
            self.pend('R', 'move-filter', f'{d.name} sets list_id {new} without filtering on the source list')
        owners = collections.defaultdict(set)
        for lid, ld in d.pass_store.get('lists', {}).items():
            if ld.get('cloud_id') is not None:
                owners[str(ld['cloud_id'])].add(lid)
        c = self.ctx
        lenient = c is not None and c.same_edit_fired and d.copy is None   # an edit of the store the pass reads
        items = dict(d.pass_store.get('items', {}))
        if lenient:
            for iid, it in c.pass_start_items.items():
                items.setdefault(iid, it)   # an item the edit removed during the pass
        for r in rows:
            if _eq(r['list_id'], new):
                continue
            ok = False
            for iid, it in items.items():
                if not self._names_row(it, r['id']):
                    continue
                now = set(it.get('lists', []))
                then = set(c.pass_start_lists.get(iid, ())) if (lenient and c) else now
                dest = owners.get(str(new), set())
                source = owners.get(str(r['list_id']), set())
                in_dest = bool((now | then) & dest) if lenient else bool(now & dest)
                in_src = bool(now & then & source) if lenient else bool(now & source)
                # a moved row bound for the destination goes there even from a list the entry is in
                # (another computer moved it into one): it waits for no other membership
                bound = any(str(k).startswith('~') and _eq(rc.get('id'), r['id']) and rc.get('to') in dest
                            for k, rc in _records(it))
                if in_dest and (not in_src or bound):
                    ok = True
            if not ok:
                self.pend(2, 'row-moved', f'{d.name} moved row {r["id"]} ({r["sys_id"]},{r.get("fl_id")}) from '
                                          f'list {r["list_id"]} to {new} without a desktop move of that entry')

    def _check_fill(self, d, r, p):
        prev = row_ident(r, True)
        name = d.name
        if 'sys_id' in p and str(p['sys_id']) != prev[0]:
            self.pend(2, 'sys-changed', f'{name} changed row {r["id"]} sys_id {prev[0]} -> {p["sys_id"]}')
        new_fl = norm(p['fl_id']) if 'fl_id' in p else prev[1]
        new_pg = norm(p['page']) if 'page' in p else prev[2]
        if prev[1] and new_fl != prev[1]:
            self.pend(2, 'fl-replaced', f'{name} changed row {r["id"]} fl_id {prev[1]} -> {new_fl}')
        if prev[2] and new_pg != prev[2]:
            self.pend(2, 'page-replaced', f'{name} changed row {r["id"]} page {prev[2]} -> {new_pg}')
        filled = (not prev[1] and new_fl) or (not prev[2] and new_pg)
        if filled:
            new = (prev[0], new_fl, new_pg)
            if not same_entry(new, prev, self.db.has_page):
                self.pend(2, 'fill-other-entry', f'{name} gave row {r["id"]} {prev} the identity {new}')
            ghost = r.get('_ghost')
            if ghost and ghost[0] == 'web' and not ghost[2]:
                self.pend(2, 'web-row-filled', f'{name} gave website row {r["id"]} {prev} the identity {new}')

    def _check_insert(self, d, p):
        c = self.ctx
        if c is None:
            return
        # once this pass found the column missing, the desktop compares as without it
        hp = self.db.has_page and not d.client.page_missing
        e = (str(p.get('sys_id')), norm(p.get('fl_id')), norm(p.get('page')))
        target = p.get('list_id')
        if any(_eq(target, x) for x in c.anon_lists):
            return  # an anonymous read looks like an empty list; nothing tells the two apart
        items = d.pass_store.get('items', {})
        for r in self.db.visible('list_items', d.client.session_user):
            if not _eq(r['list_id'], target):
                continue
            # a row this desktop inserted earlier in this step (a batch that committed
            # although its response was lost): inserting the entry again duplicates it
            ghost = r.get('_ghost')
            again = r['id'] not in c.start_rows and ghost is not None and ghost[0] == d.name
            if not again and not _eq(c.start_rows.get(r['id']), target):
                continue
            if not same_entry(e, row_ident(r, hp), hp):
                continue
            if again:
                # ... unless its answer came back and was recorded for one membership of the
                # entry while another membership of it in this list still has no row: that
                # one's own insert (a batch that wrote nothing, retried one row per request)
                if not (any(self._names_row(it, r['id']) for it in items.values())
                        and self._needs_a_row(d, target, e, hp)):
                    self.pend(4, 'insert-again', f'{d.name} inserted {e} into cloud list {target} again in one '
                                                 f'step: row {r["id"]} {row_ident(r, hp)} is its own earlier insert')
                continue
            if any(self._names_row(it, r['id'], include_gone=True) for it in d.pass_store.get('items', {}).values()):
                continue
            if str(r['id']) in (d.pass_store.get('cloud_deletes') or {}):
                continue    # a removed entry's row, waiting for its delete: never paired again
            self.pend(4, 'insert-unclaimed', f'{d.name} inserted {e} into cloud list {target}, which already held '
                                             f'row {r["id"]} {row_ident(r, hp)} that no record of {d.name} names')

    @staticmethod
    def _needs_a_row(d, target, e, hp):
        """A membership of entry e in the local list that owns cloud list `target` holds no record."""
        owners = {lid for lid, ld in d.pass_store.get('lists', {}).items() if _eq(ld.get('cloud_id'), target)}
        for iid, it in d.pass_store.get('items', {}).items():
            for lid in owners & set(it.get('lists') or []):
                if not isinstance((it.get('cloud_rows') or {}).get(lid), dict) and same_entry(ident(iid, it), e, hp):
                    return True
        return False

    # ---- website actor (the writes of web/supabase_client.py and web/user_lists.py)
    def web_lists(self, live_only=True):
        return [lst for lst in self.db.tables['user_lists']
                if lst['user_id'] == USER and not (live_only and lst.get('deleted_at'))
                and lst['name'] != 'Recently Viewed']

    def web_remove(self, r):
        """delete_list_item: one row, by id."""
        self.web.table('list_items').delete().eq('id', r['id']).execute()
        self.unrevive({r['id']})
        if self.ctx is not None:
            self.ctx.web_deleted.add(r['id'])
        self.trace.append(f'web: remove row {r["id"]} ({r["sys_id"]},{r.get("fl_id")})')
        self.retire_if_lost()

    def web_add(self, lst, sel):
        """add_list_item: an entry into a cloud list (it may duplicate one already there)."""
        sys_id = SYS[sel[2] % len(SYS)]
        fl = (None, 'FLa', 'FLb')[sel[3] % 3]
        note = self.fresh('n') if sel[4] % 2 else ''
        if note:
            self.live_n.add(note)
        # the website writes the catalogue's shelfmark and title (web/user_lists.py add_item)
        self.web.table('list_items').insert({'list_id': lst['id'], 'sys_id': sys_id,
                                             'shelfmark': web_shelfmark(sys_id), 'title': web_title(sys_id),
                                             'fl_id': fl, 'note': note, 'tags': []}).execute()
        self.trace.append(f'web: add ({sys_id},{fl}) note={note!r} to cloud list {lst["id"]} {lst["name"]}')

    def web_rename(self, sel):
        """update_list: the website renames a live list, writing name and name_en (web/user_lists.py)."""
        wl = self.web_lists()
        if not wl:
            return
        lst = wl[sel[1] % len(wl)]
        # mostly a name another list may have; sometimes one no list has
        nm = NAMES[sel[2] % len(NAMES)] if sel[3] % 4 else f'R{sel[2] % 5}'
        old = lst['name']
        if nm == old:
            return
        self.web.table('user_lists').update({'name': nm, 'name_en': nm}).eq('id', lst['id']).execute()
        self.trace.append(f'web: rename cloud list {lst["id"]} {old} -> {nm}')

    def web_op(self, sel):
        kind = sel[0] % 9
        rows = sorted((r for r in self.db.tables['list_items'] if self.db.owner('list_items', r) == USER),
                      key=lambda r: r['id'])
        wl = self.web_lists()
        if kind in (0, 1) and wl:                        # add_list_item (may duplicate an entry)
            self.web_add(wl[sel[1] % len(wl)], sel)
        elif kind == 2 and rows:                         # note edit: replace or append
            r = rows[sel[1] % len(rows)]
            t = self.fresh('n')
            self.live_n.add(t)
            old = r.get('note')
            new = t if sel[2] % 2 else ((old or '') + '\n' + t).lstrip('\n')
            self.live_n -= toks(old) - toks(new)
            self.web.table('list_items').update({'note': new}).eq('id', r['id']).execute()
            self.trace.append(f'web: row {r["id"]} ({r["sys_id"]},{r.get("fl_id")}) note {_short(old)} -> '
                              f'{_short(new)}')
        elif kind == 3 and rows:                         # tag edit: add or remove
            r = rows[sel[1] % len(rows)]
            cur = list(r.get('tags') or [])
            if cur and sel[2] % 2:
                gone = cur.pop(sel[3] % len(cur))
                self.live_g.discard(gone)
            else:
                t = self.fresh('g')
                self.live_g.add(t)
                cur.append(t)
            self.web.table('list_items').update({'tags': cur}).eq('id', r['id']).execute()
            self.trace.append(f'web: row {r["id"]} tags -> {cur}')
        elif kind == 4 and rows:                         # remove one entry, by row id
            self.web_remove(rows[sel[1] % len(rows)])
        elif kind == 5 and wl:                           # a list to the Trash
            lst = wl[sel[1] % len(wl)]
            self.web.table('user_lists').update({'deleted_at': '2026-09-27T00:00:00+00:00'}).eq(
                'id', lst['id']).execute()
            self.trace.append(f'web: trash cloud list {lst["id"]} {lst["name"]}')
        elif kind == 6:                                  # restore a trashed list, or create one
            trashed = [lst for lst in self.web_lists(False) if lst.get('deleted_at')]
            if trashed and sel[1] % 2:
                lst = trashed[sel[2] % len(trashed)]
                self.web.table('user_lists').update({'deleted_at': None}).eq('id', lst['id']).execute()
                self.trace.append(f'web: restore cloud list {lst["id"]} {lst["name"]}')
            else:
                nm = NAMES[sel[2] % len(NAMES)]
                new = self.web.table('user_lists').insert({'user_id': USER, 'name': nm, 'name_en': nm,
                                                           'color': '#FFD700', 'is_default': False,
                                                           'is_system': False}).execute().data[0]
                self.trace.append(f'web: create cloud list {new["id"]} {nm}')
        elif kind == 7 and self.web_lists(False) and sel[1] % 3 == 0:   # delete a list (cascade)
            every = self.web_lists(False)
            lst = every[sel[2] % len(every)]
            gone = [r['id'] for r in rows if r['list_id'] == lst['id']]
            self.web.table('user_lists').delete().eq('id', lst['id']).execute()
            self.unrevive(set(gone))
            if self.ctx is not None:
                self.ctx.web_deleted |= set(gone)
            self.trace.append(f'web: delete cloud list {lst["id"]} {lst["name"]} permanently')
            self.retire_if_lost()

        elif kind == 8 and len(rows) > 1:                # paste one row's note onto another
            src, dst = rows[sel[1] % len(rows)], rows[sel[2] % len(rows)]
            # a paste that leaves the note as it was is no write the desktops could see
            if src['id'] != dst['id'] and (src.get('note') or '') != (dst.get('note') or ''):
                old = dst.get('note')
                for t in toks(src.get('note')) - self.live_n:
                    self.revived[t] = dst['id']
                self.live_n |= toks(src.get('note'))
                self.live_n -= toks(old) - toks(src.get('note'))
                self.web.table('list_items').update({'note': src.get('note') or ''}).eq('id', dst['id']).execute()
                self.trace.append(f'web: paste the note of row {src["id"]} onto row {dst["id"]} '
                                  f'({_short(old)} -> {_short(src.get("note"))})')

    # ---- the removal ledger (invariant 11) and the harness's own map of moves
    def _ledger_before(self, d):
        """What a user op starts from, read from the desktop's membership records (never a '~' record)."""
        if not self.checking:
            return None
        data = d.data
        recs = {}
        for iid, it in data['items'].items():
            for k, r in _records(it):
                if not str(k).startswith('~') and not r.get('gone') and r.get('id') is not None:
                    recs[(iid, k)] = (str(r['id']), r.get('list'))
        idents = {iid: ident(iid, it) for iid, it in data['items'].items()}
        return self._pairs(d), recs, idents, set(data['items']), data.get('cloud_account')

    def _ledger_after(self, d, snap, kind, dest=None):
        """Moves go into the move map; removals into the ledger; an entry put back is noted (O-12)."""
        if snap is None or not self.checking:
            return
        self.user_ops += 1
        members0, recs0, idents0, items0, account = snap
        members1 = self._pairs(d)
        lost = members0 - members1
        where = {str(r['id']): r['list_id'] for r in self.db.tables['list_items']}
        hp = self.db.has_page
        if kind in MOVE_KINDS:
            for iid, lid in sorted(lost):
                to = dest(iid) if callable(dest) else dest
                if to is None:
                    continue
                if d.upload_id is not None:
                    # rows the running upload writes for this entry in the list it left follow it too:
                    # mapped once that upload is installed (_map_upload_moves)
                    self.upload_moves.append((d.name, d.upload_id, iid, idents0[iid], lid, to))
                for key, val in list(self.moves.items()):
                    if key[0] == d.name and val[:2] == (iid, lid):
                        self.moves[key] = (iid, to, val[2], val[3]) + val[4:]
                rec = recs0.get((iid, lid))
                if rec is not None:       # (one already gone from the website still explains a DELETE)
                    self.moves[(d.name, rec[0])] = (iid, to, rec[1], lid)
        elif kind in REMOVAL_KINDS or kind == 'prompt':
            for iid, lid in sorted(lost):
                rows, came_from = self._moved_rows(d, iid, lid, where)
                rec = recs0.get((iid, lid))
                if rec is not None:
                    rows[rec[0]] = rec[1]      # (one already gone from the website explains a DELETE)
                twins = {i for i, l in members1 if l == lid and i != iid and i in idents0
                         and _alike(idents0[i], idents0[iid], hp)}
                self._ledger_add(d, iid, idents0[iid], lid, rows, account, twins, came_from)
            for iid in sorted(items0 - set(d.data['items'])):
                rows, came_from = self._moved_rows(d, iid, None, where)
                rows.update({rec[0]: rec[1] for (i, _), rec in recs0.items() if i == iid})
                self._ledger_add(d, iid, idents0[iid], None, rows, account, set(), came_from)
        gained = members1 - members0
        if gained:
            now = d.data['items']
            for e in self.ledger:
                if e['desk'] != d.name or e['readded'] or e['cancelled']:
                    continue
                back = {l for i, l in gained if i in now and _alike(ident(i, now[i]), e['ident'], hp)}
                if back and (e['list'] is None or e['list'] in back):
                    e['readded'] = True
                    self.trace.append(f'    ledger: {e["iid"]} is back in {e["list"]} before its removal was '
                                      f'sent; the website row may stay')
                    # O-12: the very entry, as it was, back in that list before its delete went (and no
                    # upload running to send it): its rows are its own again, and no DELETE may take them
                    if (e['list'] is not None and (e['iid'], e['list']) in gained and d.copy is None
                            and ident(e['iid'], now[e['iid']]) == e['ident']
                            and d.data.get('cloud_account') == e['account']):
                        e['o12'] = set(e['rows']) | {str(x['rid']) for x in self._log_for(e) if x['answered']}
                        for rid, src in e['came_from'].items():     # its moved rows are on their way again
                            self.moves[(d.name, rid)] = (e['iid'], e['list'], e['rows'].get(rid), src)
                # put back in the list a moved row was leaving: that row may stay there
                e['kept'] |= {rid for rid, src in e['came_from'].items() if src in back}

    def _map_upload_moves(self, d, upload_id, recorded=None):
        """The moves made during that upload, over the rows it inserted or moved into the list left.

        recorded: {(item, list left): the upload's record there}. A row the upload paired with no
        request (a content match) is known only from that record, so it can explain a later DELETE
        (the move map's 'explain only' rows) but is never a row a removal is expected to take.
        """
        if not self.checking:
            return
        moves = [m for m in self.upload_moves if m[0] == d.name and m[1] == upload_id]
        self.upload_moves = [m for m in self.upload_moves if not (m[0] == d.name and m[1] == upload_id)]
        for _, _, iid, idn, frm, to in moves:
            for key, val in list(self.moves.items()):
                if key[0] == d.name and val[:2] == (iid, frm):
                    self.moves[key] = (iid, to, val[2], val[3]) + val[4:]
            for x in self.row_log:
                if x['desk'] == d.name and x['upload'] == upload_id and x['local'] == frm and x['answered']                         and not x['ambiguous'] and _ident_matches(idn, x['ident']):
                    self.moves.setdefault((d.name, str(x['rid'])), (iid, to, x['target'], frm))
            rec = (recorded or {}).get((iid, frm))
            if isinstance(rec, dict) and not rec.get('gone') and rec.get('id') is not None:
                self.moves.setdefault((d.name, str(rec['id'])), (iid, to, rec.get('list'), frm, 'explain only'))

    def _prune_moves(self, d):
        """A moved row leaves the map once a membership record of its entry names it: it arrived."""
        items = d.data.get('items') or {}
        for key, val in list(self.moves.items()):
            if key[0] != d.name:
                continue
            it = items.get(val[0]) or {}
            if any(not str(k).startswith('~') and not r.get('gone') and str(r.get('id')) == key[1]
                   for k, r in _records(it)):
                del self.moves[key]

    def _moved_rows(self, d, iid, lid, where):
        """Rows the move map holds for this item bound for that list (any list when lid is None); taken off it.

        A row leaves the map once the website no longer has it or a membership record of the
        item names it (it reached a list, or stayed in the one the entry was put back in).
        Returns ({row id: cloud list it was in}, {row id: the local list it was leaving}).
        """
        it = d.data['items'].get(iid) or {}
        named = {str(r.get('id')) for k, r in _records(it) if not str(k).startswith('~') and not r.get('gone')}
        rows, came_from = {}, {}
        for key, val in list(self.moves.items()):
            miid, mlid, knew, src = val[:4]
            if key[0] == d.name and miid == iid and (lid is None or mlid == lid):
                del self.moves[key]
                if key[1] in named:
                    continue
                if val[4:]:
                    self.explain_only.setdefault((d.name, iid), set()).add(key[1])
                else:
                    rows[key[1]], came_from[key[1]] = knew, src
        return rows, came_from

    def _ledger_add(self, d, iid, idn, lid, rows, account, twins, came_from=None):
        e = {'desk': d.name, 'iid': iid, 'ident': idn, 'list': lid, 'step': self.cur_step, 'account': account,
             'upload': d.upload_id, 'upload_user': d.user, 'rows': rows,
             'explains': set(self.explain_only.pop((d.name, iid), ())), 'readded': False,
             'cancelled': False, 'twins': twins, 'came_from': dict(came_from or {}), 'kept': set(),
             'op': self.user_ops, 'o12': set()}
        self.ledger.append(e)

    def _cancel_ledger_since(self, d, n, moves):
        """A backup recovery: the removals made since that snapshot are undone with it, and the
        moves pending then are pending again."""
        for e in self.ledger[n:]:
            if e['desk'] == d.name:
                e['cancelled'] = True
        for key in [k for k in self.moves if k[0] == d.name]:
            del self.moves[key]
        self.moves.update(moves)

    @contextmanager
    def _attributing(self, d):
        """While an upload is installed: ids the replay of a removal queues explain that removal's DELETEs."""
        names = [n for n in ('_queue_cloud_delete', '_forget_item') if callable(getattr(lm_mod, n, None))]
        if not names or not self.checking:
            yield
            return
        saved = {n: getattr(lm_mod, n) for n in names}

        def wrap(name, fn):
            def helper(state, item_id, *args, **kw):
                before = set(state.get('cloud_deletes') or {})
                out = fn(state, item_id, *args, **kw)
                new = {str(k) for k in set(state.get('cloud_deletes') or {}) - before}
                lid = args[0] if name == '_queue_cloud_delete' and args else None
                for e in self.ledger:
                    if new and e['desk'] == d.name and e['iid'] == item_id and (lid is None or e['list'] in (lid, None)):
                        e['explains'] |= new
                return out
            return helper
        for n in names:
            setattr(lm_mod, n, wrap(n, saved[n]))
        try:
            yield
        finally:
            for n in names:
                setattr(lm_mod, n, saved[n])

    def _log_for(self, e):
        """Rows the upload running at a removal inserted or moved for that entry (expected rows (b))."""
        if e['upload'] is None:
            return []
        return [x for x in self.row_log if x['desk'] == e['desk'] and x['upload'] == e['upload']
                and (e['list'] is None or x['local'] == e['list']) and not x['ambiguous']
                and _ident_matches(e['ident'], x['ident'])]

    def _check_o12(self, d, rid):
        """Invariant 11, O-12: an entry put back in its list before its delete went keeps its website row."""
        rid = str(rid)
        for e in self.ledger:
            if e['desk'] != d.name or e['cancelled'] or rid not in e['o12']:
                continue
            later = [x for x in self.ledger if x['desk'] == d.name and x['op'] > e['op'] and not x['cancelled']
                     and (rid in x['rows'] or rid in x['explains']
                          or any(str(y['rid']) == rid for y in self._log_for(x)))]
            if not later:
                self.pend(11, 'readd-deleted', f'{d.name} deleted row {rid} of {e["iid"]}, which was put back in '
                                               f'{e["list"]} before its removal was sent')

    def _explained(self, d, rid):
        rid = str(rid)
        for e in self.ledger:
            if e['desk'] != d.name:
                continue
            if rid in e['rows'] or rid in e['explains'] or any(str(x['rid']) == rid for x in self._log_for(e)):
                return True
        return False

    def check_removals(self):
        """Invariant 11 at the settle: no row a removal expected to go is still on the website."""
        rows = {str(r['id']): r for r in self.db.tables['list_items']}
        moved_by = collections.defaultdict(list)
        for x in self.row_log:
            if x['kind'] == 'update':
                moved_by[str(x['rid'])].append(x)
        for e in self.ledger:
            if e['cancelled'] or e['readded']:
                continue
            expected = {}
            if e['account'] == USER:
                expected.update(e['rows'])
            if e['upload_user'] == USER:
                for x in self._log_for(e):
                    if x['answered'] and x['upload'] not in self.killed:
                        expected.setdefault(str(x['rid']), x['target'])
            d = self.desks[e['desk']]
            for rid, knew in sorted(expected.items()):
                row = rows.get(rid)
                if row is None or rid in e['kept']:
                    continue
                if any((x['desk'] != e['desk'] or not x['answered'] or x['upload'] in self.killed
                        or x['n'] < self.forgot.get(e['desk'], -1)) and not _eq(x['target'], knew)
                       for x in moved_by.get(rid, ())):
                    # moved to another list by another computer -- or by this one in a way it no longer
                    # knows of: the move's answer was lost, its upload was killed, a backup came back
                    continue
                if e['twins'] and any(i in d.data['items'] and e['list'] in (d.data['items'][i].get('lists') or [])
                                      and _eq(((d.data['items'][i].get('cloud_rows') or {}).get(e['list']) or {})
                                              .get('id'), rid) for i in e['twins']):
                    continue    # the same entry, still in that list, holds the row
                self.viol(11, 'removal-lost', f'settle: {e["desk"]} removed {e["iid"]} from '
                                              f'{e["list"] or "every list"} at step {e["step"]}, but row {rid} '
                                              f'is still in cloud list {row["list_id"]}')

    def readd(self, d):
        """Put the entry this desktop removed last back in the list it left (the self-test mix)."""
        e = next((x for x in reversed(self.ledger) if x['desk'] == d.name and x['list'] is not None
                  and not x['cancelled'] and x['list'] in d.data['lists']), None)
        if e is None:
            return
        sys_id, fl, page = e['ident']
        iid = d.lm._build_item_id(sys_id, img=page, fl_id=fl)
        if iid in d.data['items'] and fl and norm(d.data['items'][iid].get('fl_id')) != fl:
            d.reidentified.add(iid)      # an item of that key already there takes this folio (as add_item does)
        snap = self._ledger_before(d)
        added = d.lm.add_item(sys_id, e['list'], fl_id=fl, img=page)
        self._ledger_after(d, snap, 0)
        self.trace.append(f'{d.name}: put {sys_id} (fl={fl} page={page}) back in {e["list"]} -> {added}')

    # ---- the website-removal prompt
    def pending_web_removals(self, d):
        f = getattr(d.lm, 'pending_web_removals', None)
        return sorted(f()) if f is not None else []

    def prompt(self, d, sel, during_pass=False, only=None):
        """Answer the website-removal prompt: every entry at once, or each on its own (Remove / Keep / later).

        only: the entries the prompt listed when it was shown (an answer made later applies to those).
        """
        pend = self.pending_web_removals(d)
        if only is not None:
            pend = [p for p in pend if p in only]
        if not pend:
            return
        mode = sel[1] % 4
        choices = {}
        for n, key in enumerate(pend):
            ch = ('remove', 'keep')[mode] if mode < 2 else ('remove', 'keep', None)[(sel[2] // 3 ** (n % 9)) % 3]
            if ch:
                choices[tuple(key)] = ch
        snap = self._ledger_before(d)
        out = d.lm.resolve_web_removals(choices)
        self._ledger_after(d, snap, 'prompt')
        tag = f'{d.name}{" (during its pass)" if during_pass else ""}'
        self.trace.append(f'{tag}: website-removal prompt {sorted(choices.items())} -> {out}')
        self.retire_if_lost()

    def _offer(self, d):
        """Invariant 13: a sync end offers the prompt only as the harness's own gate says."""
        gate = _gate()
        if gate is None or not self.pending_web_removals(d):
            return
        f = d.ui
        allowed = gate(f['visible'], f['restoring'], f['modal_open'], f['logout_pending'], f['close_pending'])
        oracle = (f['visible'] and not f['restoring'] and not f['modal_open'] and not f['logout_pending']
                  and not f['close_pending'])
        if bool(allowed) != oracle:
            self.viol(13, 'prompt-gate', f'{d.name}: the dialog gate said {allowed} with {f}')
        if allowed:
            self.trace.append(f'    {d.name}: the website-removal prompt is offered')

    # ---- desktop user ops
    def user_lists(self, d):
        return [lid for lid, lst in d.data['lists'].items() if lid != 'recent' and not lst.get('is_system')]

    def desk_op(self, d, sel, during_pass=False, pinned=None):
        snap = self._ledger_before(d)
        kind, dest = self._desk_op(d, sel, during_pass, pinned)
        self._ledger_after(d, snap, kind, dest)

    def pin(self, d, sel):
        """What an edit decides from the cloud identity when it is made (the automatic duplicate
        merge prefers a list with no cloud id), so the twin world can make the same edit later."""
        if sel == LAST_WRITE:
            return {'remove': self.last_write(d)}
        if sel[0] % 12 != 8 or not sel[2] % 2:
            return None
        groups = [g for g in d.lm.find_duplicate_lists() if g['name'] not in ('General', 'Recently Viewed')]
        if not groups:
            return None
        g = groups[sel[1] % len(groups)]
        first = sorted(g['lists'], key=lambda x: (0 if x['project_id'] else 1, 0 if not x['has_cloud_id'] else 1,
                                                  x['created']))[0]
        return {'keep': first['id']}

    def _desk_op(self, d, sel, during_pass=False, pinned=None):
        """One user edit; returns (its kind, the list a move put the entries in -- or how to find it)."""
        dest = None
        lm, data = d.lm, d.data
        kind = sel[0] % 12
        if during_pass and kind == 10:
            kind = 5   # no backup recovery in the middle of a pass
        lists = self.user_lists(d)
        order = {lid: n for n, lid in enumerate(data['lists'])}
        mems = sorted(((iid, lid) for iid, it in data['items'].items() for lid in it.get('lists', [])
                       if lid != 'recent'), key=lambda m: (m[0], order.get(m[1], 1 << 30)))
        tag = f'{d.name}{" (during its pass)" if during_pass else ""}'
        if kind in (0, 1, 2) and lists:                  # add an entry (1 in 23 is My Library)
            lid = lists[sel[1] % len(lists)]
            sys_id = LOCAL_SYS if sel[2] % 23 == 0 else SYS[sel[2] % len(SYS)]
            img, fl = SHAPES[sel[3] % len(SHAPES)]
            note = self.fresh('n') if sel[4] % 2 else ''
            tags = [self.fresh('g')] if sel[4] % 3 == 0 else []
            iid = lm._build_item_id(sys_id, img=img, fl_id=fl)
            existed = iid in data['items']
            if existed and fl and norm(data['items'][iid].get('fl_id')) != fl:
                d.reidentified.add(iid)
            lm.add_item(sys_id, lid, note=note, tags=tags, fl_id=fl, img=img)
            self.trace.append(f'{tag}: add {iid} (fl={fl}) note={note!r} tags={tags} to {lid}'
                              + (' [existing item]' if existed else ''))
            if not existed:
                if note:
                    self.live_n.add(note)
                self.live_g |= set(tags)
        elif kind == 3 and mems:                         # remove from one list
            iid, lid = mems[sel[1] % len(mems)]
            lm.remove_item_from_list(iid, lid)
            self.trace.append(f'{tag}: remove {iid} from {lid}')
            self.retire_if_lost()
        elif kind == 4 and mems and lists:               # move to a list (it may hold the entry already)
            iid, lid = mems[sel[1] % len(mems)]
            to = lists[sel[2] % len(lists)]
            if to != lid:
                lm.move_items_to_list([iid], lid, to)
                dest = to
                self.trace.append(f'{tag}: move {iid} {lid} -> {to}')
        elif kind == 5 and data['items']:                # note edit: replace or append
            iid = sorted(data['items'])[sel[1] % len(data['items'])]
            it = data['items'][iid]
            t = self.fresh('n')
            self.live_n.add(t)
            new = t if sel[2] % 2 else ((it.get('note') or '') + '\n' + t).lstrip('\n')
            self.live_n -= toks(it.get('note')) - toks(new)
            self.trace.append(f'{tag}: note of {iid} {_short(it.get("note"))} -> {_short(new)}')
            lm.update_item(iid, note=new)
        elif kind == 6 and data['items']:                # tag edit: add or remove
            iid = sorted(data['items'])[sel[1] % len(data['items'])]
            cur = list(data['items'][iid].get('tags') or [])
            if cur and sel[2] % 2:
                gone = cur.pop(sel[3] % len(cur))
                self.live_g.discard(gone)
            else:
                t = self.fresh('g')
                self.live_g.add(t)
                cur.append(t)
            lm.update_item(iid, tags=cur)
            self.trace.append(f'{tag}: tags of {iid} -> {cur}')
        elif kind == 7:                                  # create a list (names repeat, as on the desktop)
            nm = NAMES[sel[1] % len(NAMES)]
            new = lm.create_list(nm)
            self.trace.append(f'{tag}: create list {new} {nm}')
        elif kind == 8:                                  # Clean up duplicate lists
            groups = [g for g in lm.find_duplicate_lists() if g['name'] not in ('General', 'Recently Viewed')]
            if groups:
                g = groups[sel[1] % len(groups)]
                if sel[2] % 2 and (pinned or {}).get('keep'):
                    # the twin world: the same merge the automatic choice made when the edit was made
                    keep = pinned['keep']
                    lm.merge_duplicate_group(keep, [x['id'] for x in g['lists'] if x['id'] != keep])
                    dest = keep
                    self.trace.append(f'{tag}: clean up duplicates of {g["name"]} (auto, as made), keep {keep}')
                elif sel[2] % 2:
                    r = lm.auto_merge_duplicate_group(g)
                    dest = r.get('keep_id')
                    self.trace.append(f'{tag}: clean up duplicates of {g["name"]} (auto), keep {r.get("keep_id")}')
                else:
                    ids = [x['id'] for x in g['lists']]
                    keep = ids[sel[3] % len(ids)]
                    lm.merge_duplicate_group(keep, [i for i in ids if i != keep])
                    dest = keep
                    self.trace.append(f'{tag}: clean up duplicates of {g["name"]}, keep {keep}')
        elif kind == 9 and lists:                        # Trash / Restore / delete permanently
            lid = lists[sel[1] % len(lists)]
            if data['lists'][lid].get('deleted_at'):
                if sel[2] % 3 == 0:
                    lm.delete_list(lid, permanent=True)
                    self.trace.append(f'{tag}: delete list {lid} permanently')
                    self.retire_if_lost()
                else:
                    lm.restore_list(lid)
                    self.trace.append(f'{tag}: restore list {lid}')
            elif lid != 'default':
                lm.delete_list(lid)
                self.trace.append(f'{tag}: trash list {lid}')
        elif kind == 10 and d.snaps:                     # backup recovery: an older snapshot comes back
            k = sel[1] % len(d.snaps)
            lm.data = copy.deepcopy(d.snaps[k])
            self.set_renames(d, d.snap_renames[k])
            if k < len(d.snap_ledger):
                self._cancel_ledger_since(d, *d.snap_ledger[k])
            self.forgot[d.name] = len(self.row_log)   # what it did since that snapshot, it no longer knows
            lm.save()
            self.trace.append(f'{tag}: restore lists.pkl from its snapshot #{k}')
            self.retire_if_lost()
        elif kind == 11:                                 # empty the Trash
            n = lm.empty_trash()
            if n:
                self.trace.append(f'{tag}: empty the Trash ({n} list(s))')
                self.retire_if_lost()
        return kind, dest

    def desk_rename(self, d, sel):
        """Rename a list on the desktop (ListsManager.update_list, as the list menu does)."""
        lists = [lid for lid in self.user_lists(d) if not d.data['lists'][lid].get('deleted_at')]
        if not lists:
            return
        lid = lists[sel[1] % len(lists)]
        nm = NAMES[sel[2] % len(NAMES)] if sel[3] % 4 else f'D{sel[2] % 5}'
        if nm == d.data['lists'][lid].get('name'):
            return
        if self.checking:
            self.renamed[(d.name, lid)] = nm
        d.lm.update_list(lid, name=nm)       # its save keeps the record with lists.pkl
        if not self.checking:
            # the pre-2b ListsManager set no mark: the store the upgrade starts from has none
            d.data['lists'][lid].pop(RENAME_MARK, None)
            d.lm.save()
        self.trace.append(f'{d.name}: rename list {lid} -> {nm}')

    def prune_renames(self, d, names_too=False):
        """Forget the desktop renames of lists that are gone (or, after a restart, named otherwise)."""
        for key, nm in list(self.renamed.items()):
            ld = d.data['lists'].get(key[1]) if key[0] == d.name else {'name': nm}
            if ld is None or (names_too and ld.get('name') != nm):
                del self.renamed[key]

    def long_note(self, d, sel):
        data = d.data
        if not data['items']:
            return
        iid = sorted(data['items'])[sel[1] % len(data['items'])]
        it = data['items'][iid]
        t = self.fresh('n')
        self.live_n.add(t)
        new = ((it.get('note') or '') + '\n' + t + '\n' + LONG_FILLER).lstrip('\n')
        d.lm.update_item(iid, note=new)
        self.trace.append(f'{d.name}: a 9,000-byte note on {iid} (token {t})')

    # ---- state
    def memberships(self, d):
        out = set()
        for iid, it in d.data.get('items', {}).items():
            for lid in it.get('lists', []):
                if lid in d.data.get('lists', {}) and lid != 'recent':
                    out.add((lid, ident(iid, it)))
        return out

    def state(self):
        tables = {t: sorted(({k: v for k, v in r.items() if not k.startswith('_')} for r in rows),
                            key=lambda r: r['id'])
                  for t, rows in self.db.tables.items()}
        return copy.deepcopy(tables), {k: _strip(copy.deepcopy(d.data)) for k, d in self.desks.items()}

    def check_claims(self, d, where):
        count = collections.Counter()
        for iid, it in d.data.get('items', {}).items():
            for _, rec in _records(it):
                if rec.get('id') is not None and not rec.get('gone'):
                    count[str(rec['id'])] += 1
            if self.legacy_claims and it.get('cloud_id') is not None and not it.get('cloud_rows'):
                count[str(it['cloud_id'])] += 1
        twice = sorted(rid for rid, n in count.items() if n > 1)
        if twice:
            holders = [iid for iid, it in d.data['items'].items()
                       if any(self._names_row(it, rid) for rid in twice)]
            self.viol(4, 'row-claimed-twice', f'{where}: {d.name} names rows {twice} more than once '
                                              f'(items {holders})')
        pending = {str(e.get('id')) for e in (d.data.get('cloud_deletes') or {}).values() if isinstance(e, dict)}
        both = sorted(pending & set(count))
        if both:
            self.viol(4, 'recorded-and-pending', f'{where}: {d.name} both records rows {both} and waits to delete '
                                                 f'them')

    def check_account(self, d, where, before_gone=None):
        store = d.data
        for key, e in (store.get('cloud_deletes') or {}).items():
            if not isinstance(e, dict) or e.get('account') is None or str(e.get('id')) != str(key):
                self.viol(7, 'pending-without-account', f'{where}: {d.name} waits to delete {key} as {e!r}')
        recs = [(iid, rec) for iid, it in store.get('items', {}).items() for _, rec in _records(it)]
        if not recs:
            return
        acct = store.get('cloud_account')
        if not acct:
            self.viol(7, 'record-without-account', f'{where}: {d.name} holds records but no cloud_account')
            return
        for iid, rec in recs:
            if rec.get('gone'):
                continue
            row = next((r for r in self.db.tables['list_items'] if _eq(r['id'], rec.get('id'))), None)
            if row is not None and self.db.owner('list_items', row) not in (acct, None):
                self.viol(7, 'record-other-account', f'{where}: {d.name} item {iid} records row {rec.get("id")} '
                                                     f'of {self.db.owner("list_items", row)} under {acct}')
        if before_gone is not None:
            now_gone = {(iid, k) for iid, it in store.get('items', {}).items()
                        for k, rec in _records(it) if rec.get('gone')}
            new = now_gone - before_gone
            if new:
                self.viol(7, 'switch-tombstoned', f'{where}: the first pass of {d.name} after an account switch '
                                                  f'marked {sorted(new)} removed')

    # ---- the desktop's runner, as the harness drives it (no Qt)
    def upload(self, d, stop_at=None, backfill=True, gone=None):
        """An upload as the runner makes one: on a copy of the store, installed when it ends.

        stop_at: the pass is stopped at its n-th check (Cancel, or the sign-out's deadline).
        gone: ('close' | 'kill', at, applied): the program goes away at that request of the
        upload -- a close hands what the upload reported to abandon_upload and saves; a kill
        restarts from the last save.
        """
        kw = {}
        if stop_at is not None and _takes(d.sync.sync_to_cloud, 'should_stop'):
            calls = [0]

            def should_stop():
                calls[0] += 1
                return calls[0] >= stop_at
            kw['should_stop'] = should_stop
        if not backfill and _takes(d.sync.sync_to_cloud, 'backfill_pages'):
            kw['backfill_pages'] = False
        if not d.copy_path():
            if gone is not None:
                d.gone = {'at': gone[1], 'count': 0, 'applied': gone[2]}
                try:
                    return d.sync.sync_to_cloud(**kw)
                except _ProcessGone:
                    self._went_away(d, gone[0], None, None, None)
                    return {'success': False, 'error': 'gone', 'gone': gone[0]}
                finally:
                    d.gone = None
            return d.sync.sync_to_cloud(**kw)
        lm, user = d.lm, d.user
        cp, base = lm.begin_upload()
        live0 = copy.deepcopy(lm.data) if self.checking else None
        self.upload_seq += 1
        d.copy, d.reports, d.upload_id = cp, [], self.upload_seq
        c = self.ctx
        edits0 = c.same_edit_fired if c is not None else False
        if gone is not None:
            d.gone = {'at': gone[1], 'count': 0, 'applied': gone[2], 'edit': gone[3] if len(gone) > 3 else None}
        withheld = set()

        def withdrawn():
            gone = lm.withdrawn_now()
            withheld.update(gone)
            return gone
        try:
            res = d.sync.sync_to_cloud(data=cp, withdrawn=withdrawn, on_recorded=d.reports.append, **kw)
        except _ProcessGone:
            reports, upload_id = list(d.reports), d.upload_id
            d.copy = d.reports = d.upload_id = d.gone = None
            self._went_away(d, gone[0], base, reports, user, upload_id)
            return {'success': False, 'error': 'gone', 'gone': gone[0]}
        finally:
            d.gone = None
        d.copy = None
        if c is not None:
            # the upload held back a write for an entry removed meanwhile: with the removal made after
            # it, that write goes out and is undone later -- the same end on one computer, not always
            # with another one holding the row
            skipped = any(iid in cp['items'] and lid in (cp['items'][iid].get('lists') or [])
                          and not isinstance((cp['items'][iid].get('cloud_rows') or {}).get(lid), dict)
                          for iid, lid in withheld)
            c.twin_excluded = c.twin_excluded or skipped or _serial_differs(base, cp)
        before_install = None
        if self.checking:
            edited = c is not None and c.same_edit_fired and not edits0
            if not edited and _strip(copy.deepcopy(lm.data)) != _strip(live0):
                self.pend('R', 'live-changed-during-upload', f'{d.name}: the live store changed while an upload ran '
                                                             f'on its copy: {_diff_store(live0, lm.data)}')
            before_install = copy.deepcopy(lm.data)
        recorded = {(m[2], m[4]): ((cp['items'].get(m[2]) or {}).get('cloud_rows') or {}).get(m[4])
                    for m in self.upload_moves if m[0] == d.name and m[1] == d.upload_id}
        with self._attributing(d):
            lm.finish_upload(cp, base, res)
        self._map_upload_moves(d, d.upload_id, recorded)
        if before_install is not None and _without_identity(lm.data) != _without_identity(before_install):
            self.pend('R', 'install-changed-store', f'{d.name}: finish_upload changed more than the cloud identity '
                                                    f'fields: {_diff_store(before_install, lm.data)}')
        d.reports = d.upload_id = None
        if c is not None and c.deferred is not None and c.desk is d:
            (sel, asked, pinned), c.deferred = c.deferred, None
            self.trace.append(f'    (twin) {d.name}: the held edit, right after the upload was installed')
            self._same_desk_edit(d, sel, asked, pinned)
        return res

    def _went_away(self, d, how, base, reports, user, upload_id=None):
        """A close (the runner's shutdown: abandon_upload with every report, then a save) or a kill."""
        if how == 'close':
            if base is not None:
                before = copy.deepcopy(d.lm.data)
                with self._attributing(d):
                    d.lm.abandon_upload(base, reports, user)
                if self.checking and _without_identity(d.lm.data) != _without_identity(before):
                    self.pend('R', 'abandon-changed-store', f'{d.name}: abandon_upload changed more than the cloud '
                                                            f'identity fields: {_diff_store(before, d.lm.data)}')
            d.lm.save()
            self.trace.append(f'    {d.name}: closed ({len(reports or ())} report(s) handed to abandon_upload); '
                              f'restart')
        else:
            if upload_id is not None:
                self.killed.add(upload_id)
            self.trace.append(f'    {d.name}: killed; restart from the last save')
        d.restart()

    def download(self, d):
        """A download as the runner makes one: fetched (network only), then applied on the UI thread."""
        if not d.fetch_path():
            return d.sync.sync_from_cloud()
        user = d.user
        before, self.served = getattr(self, 'served', None), (d, {})
        try:
            state = d.sync.fetch_cloud_state(d.lm.remembered_row_ids(user))
        finally:
            served, self.served = self.served[1], before
        if not state.get('success'):
            return {k: v for k, v in state.items() if k != 'pass'}
        rows_at_fetch = {r['id']: dict(r) for r in self.db.tables['list_items']}
        # A row the fetch was answered with justifies what the apply adds from it, also when the
        # website removed it before the fetch ended (a keyset read asks for a last, empty page,
        # at which web_between_pages removes rows the read already returned).
        for rid, r in served.items():
            rows_at_fetch.setdefault(rid, r)
        c = self.ctx
        if c is not None and c.inject and c.inject[1] == 'between_stages' and not c.fired and c.desk is d:
            c.fired = True
            self.trace.append(f'    {d.name}: a user edit between the Download\'s fetch and its apply')
            sel = c.inject[2]
            self._same_desk_edit(d, sel, self.pending_web_removals(d) if (sel[0] // 12) % 6 == 0 else [])
        if d.user != user or d.signed_out:
            return {'success': False, 'error': 'stale: the account changed before the apply'}
        members0 = self._pairs(d)
        pending0 = {str(e.get('id')): e.get('list') for e in (d.data.get('cloud_deletes') or {}).values()
                    if isinstance(e, dict) and e.get('account') == user}
        res = d.sync.apply_cloud_state(state)
        if self.checking and res.get('success'):
            self._check_no_pending_readd(d, members0, pending0, rows_at_fetch)
        return res

    @staticmethod
    def _pairs(d):
        return {(iid, lid) for iid, it in d.data.get('items', {}).items() for lid in it.get('lists') or []
                if lid != 'recent'}

    def _check_no_pending_readd(self, d, members0, pending, rows):
        """Invariant 12: a membership the apply added has a row other than the ones pending deletion.

        pending: {row id: the cloud list its removal names}. A pending row another computer moved
        to another list is that list's by design (its move wins), so it counts as any other row there.
        """
        if not pending:
            return
        hp = self.db.has_page
        data = d.data
        for iid, lid in sorted(self._pairs(d) - members0):
            it, ld = data['items'].get(iid), data['lists'].get(lid)
            if it is None or ld is None:
                continue
            # its own cloud list and the same-name ones a Download reads for it ('General' for the default list)
            cloud = {str(cl['id']) for cl in self.db.tables['user_lists']
                     if cl['user_id'] == d.user and (_eq(cl['id'], ld.get('cloud_id')) or cl['name'] == ld.get('name')
                                                     or (lid == 'default' and cl['name'] == 'General'))}
            mine = ident(iid, it)
            why = [r for r in rows.values() if str(r['list_id']) in cloud and not str(r['sys_id']).startswith('97')
                   and _alike(mine, row_ident(r, hp and norm(r.get('page')) is not None), hp)]
            if why and all(str(r['id']) in pending and _eq(pending[str(r['id'])], r['list_id']) for r in why):
                self.pend(12, 'pending-re-added', f'{d.name}: a Download added {iid} to {lid} from row(s) '
                                                  f'{sorted(r["id"] for r in why)}, pending deletion')

    # ---- one pass, with the checks that belong around it
    def one_pass(self, d, direction, upload_kw=None):
        d.client.page_missing = False
        store0 = copy.deepcopy(d.data)
        c = self.ctx
        if c is not None:
            c.phase = direction
            c.pass_start_lists = {iid: list(it.get('lists', [])) for iid, it in d.data['items'].items()}
            c.pass_start_items = copy.deepcopy(d.data['items'])
            edits_before = c.same_edit_fired
        # a pass as another account than the one the store's records belong to
        switched = d.data.get('cloud_account') not in (None, d.user)
        gone0 = {(iid, k) for iid, it in d.data.get('items', {}).items()
                 for k, rec in _records(it) if rec.get('gone')} if switched else None
        d.offline_now = d.offline > 0
        if d.offline_now:
            d.offline -= 1
        try:
            res = self.upload(d, **(upload_kw or {})) if direction == 'up' else self.download(d)
        finally:
            d.offline_now = False
        if not isinstance(res, dict):
            raise Violation('crash', 'bad-result', f'{d.name} {direction} returned {res!r}')
        edited = c is not None and c.same_edit_fired and not edits_before
        reached = res.get('error') not in EARLY_ERRORS
        if res.get('gone'):
            return res            # the program went away: what came back is lists.pkl (checked by the step)
        if self.checking:
            if direction == 'up' and not edited and d.data is not None:
                a, b = _without_identity(store0), _without_identity(d.data)
                if a != b:
                    self.pend('R', 'upload-changed-store', f'{d.name}: an upload changed more than the cloud '
                                                           f'identity fields: {_diff_store(a, b)}')
            if direction == 'down' and not edited:
                for lid, ld in store0.get('lists', {}).items():
                    now = d.data.get('lists', {}).get(lid)
                    if ld.get('cloud_id') is None and now is not None and \
                            bool(ld.get('deleted_at')) != bool(now.get('deleted_at')):
                        self.pend('R', 'trash-changed', f'{d.name}: a Download changed the Trash state of {lid}, '
                                                        f'which held no cloud id')
            if direction == 'down' and not edited:
                for iid, it in d.data.get('items', {}).items():
                    if iid not in store0.get('items', {}) and str(it.get('sys_id') or '').startswith('97'):
                        self.pend(6, 'local-downloaded', f'{d.name}: a Download created My Library item {iid}')
            if d.data is not None:
                self._check_renames_after_pass(d, direction)
            if not edited and d.data is not None:
                self._check_names_after_pass(d, direction, store0, res, c)
        if reached and switched:
            if self.checking:
                self.flush(f'{d.name} {direction}')
                self.check_account(d, f'{d.name} {direction} after an account switch', gone0)
        self._prune_moves(d)
        if store0.get('cloud_account') is not None and d.data.get('cloud_account') != store0.get('cloud_account'):
            # another account's records went, pending moves with them: that account's next
            # Download brings the entry back where it was (a copy), by design
            for key in [k for k in self.moves if k[0] == d.name]:
                del self.moves[key]
        return res

    def _cloud_lists_of(self, user):
        return {str(cl['id']): cl for cl in self.db.tables['user_lists']
                if cl['user_id'] == user and cl['name'] != 'Recently Viewed'}

    def _check_names_after_pass(self, d, direction, store0, res, c):
        """Invariant 10 after a Download: every list without a pending rename (the harness's
        record, not the engine's flag) has the name of the cloud list it holds."""
        if direction != 'down':
            return
        # a website rename or the other desktop's pass during this one may have renamed a list since
        moved = c is not None and c.fired and c.inject[1] in ('web_rename', 'other_desktop_pass')
        # any injection may have hidden a list from the Download (an anonymous read shows none)
        seen = res.get('success') and not (c is not None and c.fired)
        if moved or not seen:
            return
        cloud = self._cloud_lists_of(d.user)
        for lid, ld in d.data.get('lists', {}).items():
            if lid == 'recent' or ld.get('is_system') or (d.name, lid) in self.renamed:
                continue
            cl = cloud.get(str(ld.get('cloud_id')))
            if cl is not None and ld.get('name') != cl['name']:
                self.pend(10, 'rename-not-adopted', f'{d.name}: after a Download list {lid} is named '
                                                    f'{ld.get("name")!r}, its cloud list {cl["id"]} {cl["name"]!r}')

    def _check_renames_after_pass(self, d, direction):
        """A pending desktop rename survives every Download (it is sent only by an upload)."""
        self.settle_inserts(d)
        self.prune_renames(d, names_too=False)
        for key, nm in list(self.renamed.items()):
            if key[0] != d.name:
                continue
            ld = d.data['lists'][key[1]]
            if direction == 'down' and ld.get('name') != nm:
                self.pend(10, 'desk-rename-lost', f'{d.name}: a Download renamed {key[1]} {nm!r} -> '
                                                  f'{ld.get("name")!r} before an upload had sent its rename')
                del self.renamed[key]

    def check_names_settled(self):
        for d in self.desks.values():
            self.settle_inserts(d)
            self.prune_renames(d, names_too=False)
        if self.renamed:
            self.viol(10, 'desk-rename-never-sent', f'settle: desktop renames never reached the cloud: '
                                                    f'{sorted(self.renamed.items())}')
        cloud = self._cloud_lists_of(USER)
        for name, d in self.desks.items():
            for lid, ld in d.data['lists'].items():
                if lid == 'recent' or ld.get('is_system'):
                    continue
                cl = cloud.get(str(ld.get('cloud_id')))
                if cl is not None and ld.get('name') != cl['name']:
                    self.viol(10, 'names-differ-at-settle', f'settle: {name} list {lid} {ld.get("name")!r} '
                                                            f'holds cloud list '
                                                            f'{cl["id"]} {cl["name"]!r}')

    def run_sync(self, d, direction):
        if direction in ('up', 'down'):
            return [self.one_pass(d, direction)]
        r1 = self.one_pass(d, 'down')
        if not r1.get('success'):
            return [r1]
        return [r1, self.one_pass(d, 'up')]

    # ---- a step
    def step(self, i, op):
        if self.switch_at and i == self.switch_at:
            self.switch_engine()
        self.cur_step = i
        kind = op[0]
        where = f'step {i}'
        if kind == 'web':
            self.web_op(op[1])
            self.check_tokens(f'{where} web')
            return
        if kind == 'web_rename':
            self.web_rename(op[1])
            return
        if kind == 'migrate':
            if self.db.migrate():
                self.trace.append('web: the page column is added' + (' (schema cache lags)'
                                                                     if self.db.page_lag else ''))
            return
        d = self.desks[op[1]]
        if kind == 'desk':
            self.desk_op(d, op[2])
            self.prune_renames(d)
            self.check_tokens(f'{where} {d.name} user op')
            return
        if kind == 'long':
            self.long_note(d, op[2])
            return
        if kind == 'desk_rename':
            self.desk_rename(d, op[2])
            return
        if kind == 'restart':
            d.restart()
            self.prune_renames(d, names_too=True)
            self.trace.append(f'{d.name}: restart (lists.pkl as last saved)')
            self.retire_if_lost()
            return
        if kind == 'account':
            if d.user != op[2] or d.signed_out:
                d.sign_in(op[2])
                self.trace.append(f'{d.name}: sign in as {op[2]}')
            return
        if kind == 'offline':
            d.offline = op[2]
            self.trace.append(f'{d.name}: offline for its next {op[2]} pass(es)')
            return
        if kind == 'ui':
            d.ui = dict(zip(sorted(UI_DEFAULT), op[2]))
            self.trace.append(f'{d.name}: window {d.ui}')
            return
        if kind == 'prompt':
            self.prompt(d, op[2])
            self.prune_renames(d)
            self.check_tokens(f'{where} {d.name} prompt')
            return
        if kind == 'readd':
            self.readd(d)
            self.check_tokens(f'{where} {d.name} re-add')
            return
        if kind == 'signout':
            if not d.signed_out:
                self._sync_step(i, d, 'up', None, signout=op[2])
            return
        if kind in ('close', 'kill'):
            # op[3]: an edit made right before the program goes away (the self-test mix)
            self._sync_step(i, d, 'up', None, gone=(kind, op[2]) + tuple(op[3:4]))
            return
        assert kind == 'sync', op
        if d.signed_out:
            self.trace.append(f'{d.name}: {op[2]} skipped (signed out)')
            return
        self._sync_step(i, d, op[2], op[3], op=op)

    def _sync_step(self, i, d, direction, inj, gone=None, signout=None, op=None):
        where = f'step {i} {d.name} ' + (gone[0] if gone else 'sign-out' if signout else direction)
        pre = None
        if (op is not None and inj and inj[1] in ('same_desktop_edit', 'edit_after_insert', 'edit_after_move')
                and direction in ('up', 'merge')
                and 14 in self.checks and self.checking and d.copy_path()
                # a schema cache that still lags fails whichever page write comes first: order-dependent
                and not (self.db.page_lag and self.db.page_lag_writes > 0)):
            pre = (self._clone(), _RUN['clock'][0], _RUN['urng'].getstate()) if _RUN else None
        c = _Step(d, tuple(inj) if inj else None)
        c.defer_edit = getattr(self, 'defer_edits', False)
        c.start_rows = {r['id']: r['list_id'] for r in self.db.tables['list_items']}
        before_m = self.memberships(d)
        self.db.lagged = False
        self.ctx = c
        c.counting = True
        try:
            if gone is not None:
                how, sel = gone[:2]
                edit = gone[2] if len(gone) > 2 else None
                at = 'write' if edit == LAST_WRITE else sel[0] % 14 + 1    # right after a write, or at a request
                res = [self.one_pass(d, 'up', upload_kw={'gone': (how, at, bool(sel[1] % 2), edit)})]
                if not res[0].get('gone'):     # the upload ended first: the runner installed it, then the close
                    if how == 'close':
                        d.lm.save()
                    d.restart()
                    self.trace.append(f'    {d.name}: {how} after the upload ended; restart')
            elif signout is not None:
                stop_at = signout[0] % 20 + 1 if signout[1] % 3 == 0 else None
                res = [self.one_pass(d, 'up', upload_kw={'stop_at': stop_at, 'backfill': False})]
                d.sign_out()
            else:
                res = self.run_sync(d, direction)
        finally:
            c.counting = False
            self.ctx = None
            d.client.session_user = None if d.signed_out else d.user
            d.client.url_limit = None
        self.trace.append(f'{where}' + (f' inject={inj[:2]} fired={c.fired}' if inj else '')
                          + ' -> ' + '; '.join(_summary(r) for r in res))
        if not d.signed_out:
            self._offer(d)
        if not self.checking:
            self.pending = []
            return
        self.flush(where)
        after_m = self.memberships(d)
        lists_now = d.data.get('lists', {})
        for (lid, idn) in sorted(before_m - c.same_edit_removed, key=repr):
            if lid in lists_now and not any(l2 == lid and compatible(idn, i2) for (l2, i2) in after_m):
                self.viol(3, 'membership-dropped', f'{where}: entry {idn} left local list {lid} during a sync pass')
        vanished = (set(c.start_rows) - {r['id'] for r in self.db.tables['list_items']} - c.web_deleted
                    - c.desk_deleted)
        if vanished:
            self.viol(3, 'row-vanished', f'{where}: cloud rows {sorted(vanished)} disappeared during a sync pass')
        self.check_tokens(where)
        for dd in self.desks.values():
            self.check_claims(dd, where)
            self.check_account(dd, where)
        if pre is not None and c.edit_in_upload and not c.twin_excluded:
            self._check_twin(pre, i, op)
        if op is None:
            return
        # a pass that met the lagging schema cache writes the page on the next one, by design
        if not c.fired and not self.db.lagged and all(r.get('success') for r in res):
            st = self.state()
            c2 = _Step(d, None)
            c2.start_rows = {r['id']: r['list_id'] for r in self.db.tables['list_items']}
            self.ctx = c2
            try:
                self.run_sync(d, direction)
            finally:
                self.ctx = None
            self.flush(where + ' (repeated)')
            st2 = self.state()
            if st2 != st:
                self.viol(5, 'repeat-changed', f'{where}: a second identical {direction} changed {_diff(st, st2)}')
            self.check_tokens(where + ' (repeated)')

    # ---- the settle
    def settle(self, rounds=10):
        if self.switch_at and not self.checking:
            self.switch_engine()
        self.cur_step = 'settle'
        for d in self.desks.values():
            d.offline, d.gone = 0, None
            if d.user != USER or d.signed_out:
                d.sign_in(USER)
        prev = None
        for n in range(rounds):
            last = {}
            for name, direction in (('A', 'up'), ('B', 'up'), ('A', 'down'), ('B', 'down')):
                d = self.desks[name]
                c = _Step(d, None)
                c.start_rows = {r['id']: r['list_id'] for r in self.db.tables['list_items']}
                self.ctx = c
                try:
                    last[(name, direction)] = self.one_pass(d, direction)
                finally:
                    self.ctx = None
                self.trace.append(f'settle {n + 1}: {name} {direction} -> {_summary(last[(name, direction)])}')
                where = f'settle round {n + 1} {name} {direction}'
                self.flush(where)
                self.check_tokens(where)
                self.check_claims(d, where)
                self.check_account(d, where)
            st = self.state()
            if st == prev:
                self.trace.append(f'-- settled after {n + 1} round(s)')
                self.check_reached(last)
                self.check_faithful()
                self.check_names_settled()
                self.check_removals()
                return
            prev = st
        self.viol(4, 'no-fixed-point', f'no fixed point after {rounds} settle rounds: {_diff(*self._one_more_round())}')

    def _one_more_round(self):
        a = self.state()
        for name, direction in (('A', 'up'), ('B', 'up'), ('A', 'down'), ('B', 'down')):
            self.one_pass(self.desks[name], direction)
        return a, self.state()

    # ---- invariant 14: the serialized twin
    def _clone(self):
        """A world sharing nothing with this one but the engines, with no checks of its own."""
        t = World.__new__(World)
        world, self.db.world = self.db.world, None
        try:
            t.db = copy.deepcopy(self.db)
        finally:
            self.db.world = world
        t.db.world = t
        for k, v in self.__dict__.items():
            if k not in ('desks', 'db', 'web', 'ctx', 'engine', 'fixture', 'trace', 'pending'):
                setattr(t, k, copy.deepcopy(v))
        t.engine, t.fixture, t.ctx, t.trace, t.pending = self.engine, self.fixture, None, [], []
        t.checks = set()
        t.web = FakeClient(t.db, 'web', USER)
        t.desks = {n: d.clone(t) for n, d in self.desks.items()}
        return t

    def _check_twin(self, pre, i, op):
        """The step with its same-desktop edit made right after the upload is installed settles alike."""
        self.twins += 1
        clock, urng = _RUN['clock'], _RUN['urng']
        with _same_clock():
            main = self._clone()
            main.settle()
            want = _projection(main)
        twin, at, state = pre
        with _same_clock():
            clock[0] = at
            urng.setstate(state)
            twin.defer_edits = True
            twin.step(i, op)
            twin.defer_edits = False
            twin.settle()
            got = _projection(twin)
        if got != want:
            self.viol(14, 'not-serial', f'step {i}: with the edit made after the upload instead of during it, the '
                                        f'settled state differs: {_projection_diff(want, got)}')
        self.trace.append(f'    (the same step with the edit after the upload settles alike)')

    def check_reached(self, last):
        for name, d in self.desks.items():
            up, down = last[(name, 'up')], last[(name, 'down')]
            data = d.data
            for iid, it in data['items'].items():
                if str(it.get('sys_id') or '').startswith('97'):
                    continue
                recs = it.get('cloud_rows') or {}
                for lid in it.get('lists', []):
                    ld = data['lists'].get(lid)
                    if lid == 'recent' or ld is None or ld.get('is_system') or ld.get('deleted_at'):
                        continue
                    rec = recs.get(lid)
                    if isinstance(rec, dict) and rec.get('gone'):
                        continue
                    row = None
                    if isinstance(rec, dict):
                        row = next((r for r in self.db.tables['list_items'] if _eq(r['id'], rec.get('id'))), None)
                    if row is None or not _eq(row['list_id'], ld.get('cloud_id')):
                        self.viol(8, 'membership-unreached', f'settle: {name} membership ({iid}, {lid}) has no record '
                                                             f'naming a row of its cloud list {ld.get("cloud_id")} '
                                                             f'(record {rec}, row {row})')
            if up.get('complete') is not True:
                self.viol(8, 'upload-incomplete', f'settle: the last upload of {name} is not complete: {_summary(up)}')
            if down.get('unchecked', 0):
                self.viol(8, 'download-unchecked', f'settle: the last download of {name} left '
                                                   f'{down.get("unchecked")} membership(s) unchecked')

    def check_faithful(self):
        hp = self.db.has_page
        cloud = [cl for cl in self.db.tables['user_lists'] if cl['user_id'] == USER]
        for d in self.desks.values():
            data = d.data
            lists = data['lists']
            order = [lid for lid in data.get('lists_order', []) if lid in lists]
            order += [lid for lid in lists if lid not in order]
            owned = {str(ld['cloud_id']): lid for lid, ld in lists.items() if ld.get('cloud_id') is not None}
            orphan_ids = {str(rec.get('id')) for iid, it in data['items'].items()
                          for k, rec in _records(it) if k not in it.get('lists', [])}
            # A record keeps pairing a row with its item after the user changed the
            # item's folio, or after another computer filled the row's page: it was the same
            # entry as the row was inserted.
            recorded_for = collections.defaultdict(set)
            rows_by_id = {str(r['id']): r for r in self.db.tables['list_items']}
            for iid, it in data['items'].items():
                for k, rec in _records(it):
                    if k not in it.get('lists', []) or rec.get('gone'):
                        continue
                    row = rows_by_id.get(str(rec.get('id')))
                    ghost = row.get('_ghost') if row else None
                    if iid in d.reidentified or (ghost and same_entry(
                            ident(iid, it), (ghost[1], ghost[2], ghost[3]), bool(ghost[4]))):
                        recorded_for[k].add(str(rec.get('id')))

            def named_for(cl, x):
                # a list of that name -- or, for 'General', the default list whatever it is called now
                return lists[x].get('name') == cl['name'] or (x == 'default' and cl['name'] == 'General')

            for cl in cloud:
                if cl.get('deleted_at') or cl['name'] == 'Recently Viewed':
                    continue
                lid = owned.get(str(cl['id']))
                if lid is None:   # a same-name cloud list is read for the first local list of its name
                    lid = next((x for x in order if x != 'recent' and not lists[x].get('is_system')
                                and named_for(cl, x)), None)
                if lid is None:
                    continue
                ld = lists[lid]
                if lid == 'recent' or ld.get('is_system') or ld.get('deleted_at') or ld.get('cloud_id') is None:
                    continue
                own = next((c for c in cloud if _eq(c['id'], ld['cloud_id'])), None)
                if own is None or own.get('deleted_at'):
                    continue
                members = [ident(iid, it) for iid, it in data['items'].items() if lid in it.get('lists', [])]
                for r in self.db.tables['list_items']:
                    if not _eq(r['list_id'], cl['id']) or str(r['sys_id']).startswith('97') \
                            or str(r['id']) in orphan_ids:
                        continue
                    ghost = r.get('_ghost')
                    # a row written without the column has no page unless one was filled in since
                    row_hp = hp and (ghost is None or ghost[4] or norm(r.get('page')) is not None)
                    ri = row_ident(r, row_hp)
                    ok = str(r['id']) in recorded_for[lid] or any(
                        same_entry(m, ri, True) if row_hp else (m[0], m[1]) == (ri[0], ri[1]) for m in members)
                    if not ok:
                        self.viol(9, 'row-unreached', f'settle: {d.name}: row {r["id"]} {ri} of cloud list {cl["id"]} '
                                                      f'reaches no item of local list {lid} that is the same entry '
                                                      f'(members {members})')


# --------------------------------------------------------------------------- helpers

VOLATILE = ('modified', 'added', 'created')


def _strip(data):
    for it in data.get('items', {}).values():
        for k in VOLATILE:
            it.pop(k, None)
    for lst in data.get('lists', {}).values():
        lst.pop('created', None)
    for p in data.get('projects', {}).values():
        p.pop('created', None)
    return data


def _without_identity(data, fields=IDENTITY_FIELDS):
    out = copy.deepcopy(data)
    for k in fields['store']:
        out.pop(k, None)
    for part in ('projects', 'lists', 'items'):
        for v in out.get(part, {}).values():
            for k in fields[part]:
                v.pop(k, None)
    return out


def _diff_store(a, b):
    for part in sorted(set(a) | set(b)):
        if a.get(part) != b.get(part):
            if isinstance(a.get(part), dict) and isinstance(b.get(part), dict):
                ks = sorted(k for k in set(a[part]) | set(b[part]) if a[part].get(k) != b[part].get(k))
                return f'{part} {ks[:2]}: {[a[part].get(k) for k in ks[:1]]} -> {[b[part].get(k) for k in ks[:1]]}'
            return f'{part}: {a.get(part)!r} -> {b.get(part)!r}'
    return '?'


def _diff(a, b):
    tables_a, desks_a = a
    tables_b, desks_b = b
    for t in tables_a:
        if tables_a[t] != tables_b[t]:
            ia = {r['id']: r for r in tables_a[t]}
            ib = {r['id']: r for r in tables_b[t]}
            ch = [(k, ia.get(k), ib.get(k)) for k in sorted(set(ia) | set(ib)) if ia.get(k) != ib.get(k)]
            return f'{t} {ch[:2]}'
    for k in desks_a:
        if desks_a[k] != desks_b[k]:
            return f'desk {k} {_diff_store(desks_a[k], desks_b[k])}'
    return '?'


def _short(text):
    if text and len(text) > 60:
        return repr(text[:40] + f'...<{len(text)} chars>')
    return repr(text)


def _summary(r):
    keys = ('success', 'items_pushed', 'items_failed', 'items_added', 'notes_kept', 'notes_merged',
            'tags_merged', 'notes_too_long', 'notes_differing', 'unchecked', 'waiting', 'complete')
    parts = [f'{k}={r[k]}' for k in keys if k in r and r[k] not in (0, None, [])]
    if r.get('web_removed'):
        parts.append(f'web_removed={len(r["web_removed"])}')
    if r.get('error'):
        parts.append(f'error={str(r["error"])[:60]!r}')
    return ' '.join(parts)


def _alike(a, b, has_page):
    """Two identities a sync may pair with one row: equal, certainly one entry, or -- without the page
    column -- the same folio (a page and the whole manuscript look alike there)."""
    return a == b or same_entry(a, b, has_page) or (not has_page and a[:2] == b[:2])


def _ident_matches(mine, theirs):
    """A local entry's identity against a row's (a row written without the page column has no page)."""
    return mine[0] == theirs[0] and mine[1] == theirs[1] and (theirs[2] is None or mine[2] == theirs[2])


def _serial_differs(base, cp):
    """Invariant 14's excluded uploads: those in which a delete the user queued meanwhile may, by design,
    delete a row a serial order would keep -- decided from the store the upload started from and its copy."""
    if base.get('cloud_account') != cp.get('cloud_account'):
        return True                         # an account switch
    held = collections.Counter(str(ld.get('cloud_id')) for lid, ld in (base.get('lists') or {}).items()
                               if ld.get('cloud_id') is not None and lid != 'recent')
    if any(n > 1 for n in held.values()):
        return True                         # two local lists held one cloud list: the one-owner repair
    named = collections.Counter()
    for it in (base.get('items') or {}).values():
        for rid in {str(r.get('id')) for _, r in _records(it) if not r.get('gone') and r.get('id') is not None}:
            named[rid] += 1
    return any(n > 1 for n in named.values())   # two entries named one row: the split


def _note_lines(text):
    return tuple(sorted(ln for ln in (text or '').replace('\r\n', '\n').split('\n')
                        if ln.strip() and not ln.startswith('--- ')))


def _projection(w):
    """What invariant 14 compares: the website's entries and each desktop's, by name and identity."""
    names = {str(cl['id']): cl['name'] for cl in w.db.tables['user_lists']}
    rows = collections.Counter(
        (names.get(str(r['list_id'])), str(r['sys_id']), norm(r.get('fl_id')), norm(r.get('page')),
         _note_lines(r.get('note')), frozenset(_json_set(r.get('tags') or [])))
        for r in w.db.tables['list_items'])
    desks = {}
    for n, d in w.desks.items():
        data = d.data
        desks[n] = collections.Counter(
            ((data['lists'].get(lid) or {}).get('name'), ident(iid, it), _note_lines(it.get('note')),
             frozenset(_json_set(it.get('tags') or [])))
            for iid, it in data['items'].items() for lid in it.get('lists') or [] if lid != 'recent')
    return rows, desks


def _projection_diff(concurrent, serial):
    if concurrent[0] != serial[0]:
        return f'website: only with the edit during the upload {sorted((concurrent[0] - serial[0]).items(), key=repr)[:3]}' \
               f', only with it after {sorted((serial[0] - concurrent[0]).items(), key=repr)[:3]}'
    for n in concurrent[1]:
        if concurrent[1][n] != serial[1][n]:
            return f'desk {n}: only with the edit during the upload ' \
                   f'{sorted((concurrent[1][n] - serial[1][n]).items(), key=repr)[:3]}, only with it after ' \
                   f'{sorted((serial[1][n] - concurrent[1][n]).items(), key=repr)[:3]}'
    return '?'


# --------------------------------------------------------------------------- generation, runs, shrinking

def cfg_for(seed):
    rng = random.Random(seed * 31 + 7)
    return {'has_page': rng.random() < 0.8, 'max_rows': rng.choice([None, None, 2, 3]),
            'p_inject': rng.choice([0.0, 0.1, 0.25]), 'past_end_raises': rng.random() < 0.5,
            'page_lag': rng.random() < 0.25,
            'upgrade': rng.randrange(10, 21) if rng.random() < 0.2 else 0}


def _runner_op(extra, desk, sel, i, back):
    """One of the ops the runner brings, drawn from its own stream (None: keep the desktop edit)."""
    total = sum(w for _, w in RUNNER_OPS)
    x, acc = extra.random() * total, 0.0
    for name, w in RUNNER_OPS:
        acc += w
        if x < acc:
            break
    if name == 'prompt':
        return ('prompt', desk, sel)
    if name == 'signout':
        if desk in back:
            return None
        back[desk] = i + extra.randrange(2, 6)     # signed in again a few steps later
        return ('signout', desk, sel)
    if name in ('close', 'kill'):
        return (name, desk, sel)
    if name == 'offline':
        return ('offline', desk, extra.randrange(1, 4))
    # the window: each state that holds the prompt back, now and then
    return ('ui', desk, tuple(bool(extra.random() < 0.8) if k == 'visible' else bool(extra.random() < 0.25)
                              for k in sorted(UI_DEFAULT)))


def _desk_sel(rng, kind):
    """A desk_op selector for that kind (never one the same-desktop edit takes for a prompt answer)."""
    k = rng.randrange(1, 5000)
    while k % 6 == 0:
        k += 1
    return (kind + 12 * k,) + tuple(rng.randrange(1 << 16) for _ in range(4))


def gen_removal_mix(seed, steps, cfg):
    """The self-test's op mix: removals during uploads (right after an insert was answered), a close in
    the middle of an upload, Downloads after removals, and entries put back in the list they left."""
    rng = random.Random(seed * 7919 + 5)
    ops = []
    for _ in range(steps):
        desk = rng.choice('AB')
        r = rng.random()
        if r < 0.24:
            ops.append(('desk', desk, _desk_sel(rng, 0)))                    # add an entry
        elif r < 0.30:
            ops.append(('desk', desk, _desk_sel(rng, 7)))                    # a new list
        elif r < 0.38:
            ops.append(('desk', desk, _desk_sel(rng, 3)))                    # remove from one list
        elif r < 0.42:
            ops.append(('desk', desk, _desk_sel(rng, 4)))                    # move
        elif r < 0.50:
            ops.append(('readd', desk))
        elif r < 0.70:
            kind = rng.choice(('edit_after_insert', 'edit_after_insert', 'edit_after_move', 'same_desktop_edit'))
            edit = LAST_WRITE if kind != 'same_desktop_edit' and rng.random() < 0.5 else                 _desk_sel(rng, rng.choice((3, 3, 4, 0)))
            ops.append(('sync', desk, rng.choice(('up', 'up', 'merge')), (rng.randrange(1, 12), kind, edit)))
        elif r < 0.82:
            ops.append(('sync', desk, rng.choice(('down', 'merge')), None))
        elif r < 0.90:
            sel = tuple(rng.randrange(1 << 16) for _ in range(5))
            how = rng.choice(('close', 'close', 'kill'))
            x = rng.random()
            ops.append((how, desk, sel, LAST_WRITE) if x < 0.4 else (how, desk, sel, _desk_sel(rng, 3)) if x < 0.7
                       else (how, desk, sel))
        elif r < 0.95:
            ops.append(('web', tuple(rng.randrange(1 << 16) for _ in range(5))))
        else:
            ops.append(('sync', desk, 'up', None))
    return ops


def gen_ops(seed, steps, cfg):
    if cfg.get('mix') == 'removals':
        return gen_removal_mix(seed, steps, cfg)
    rng = random.Random(seed * 7919 + 1)
    renames = random.Random(seed * 7919 + 2)
    extra = random.Random(seed * 7919 + 3)
    ops = []
    back = {}
    for i in range(steps):
        for dname in sorted(back):
            if i >= back[dname]:
                ops.append(('account', dname, USER))
                del back[dname]
        r = rng.random()
        sel = tuple(rng.randrange(1 << 16) for _ in range(5))
        desk = rng.choice('AB')
        if r < 0.20:
            if not cfg['has_page'] and rng.random() < 0.1:
                ops.append(('migrate',))
            elif renames.random() < WEB_RENAME_SHARE:
                ops.append(('web_rename', sel))
            else:
                ops.append(('web', sel))
        elif r < 0.55:
            x = rng.random()
            if x < 0.03:
                ops.append(('restart', desk))
            elif x < 0.05 and desk not in back:
                ops.append(('account', desk, OTHER_USER))
                back[desk] = i + rng.randrange(2, 6)
            elif x < 0.065:
                ops.append(('long', desk, sel))
            elif renames.random() < DESK_RENAME_SHARE:
                ops.append(('desk_rename', desk, sel))
            else:
                other = _runner_op(extra, desk, sel, i, back) if extra.random() < RUNNER_OP_SHARE else None
                ops.append(other or ('desk', desk, sel))
        else:
            direction = rng.choice(['up', 'up', 'down', 'merge'])
            inj = None
            if rng.random() < cfg['p_inject']:
                inj = (rng.randrange(1, 30), rng.choice(INJECTIONS), sel)
            ops.append(('sync', desk, direction, inj))
    return ops


def run_ops(seed, cfg, ops, engine='current', checks=ALL_CHECKS, settle=True):
    """Replay ops; return (violation or None, world)."""
    eng = load_engine(engine)
    fixture = load_engine('fixture')
    with _run_context(seed, {eng, fixture}):
        w = World(seed, cfg, eng, fixture=fixture, checks=checks)
        try:
            for i, op in enumerate(ops):
                w.step(i, op)
            if settle:
                w.settle()
            return None, w
        except Violation as v:
            return v, w
        except Exception as e:   # an engine or harness crash is a finding too
            import traceback
            tb = traceback.format_exc().strip().splitlines()[-6:]
            if 'crash' in w.checks:
                return Violation('crash', type(e).__name__, f'{type(e).__name__}: {e}\n    ' + '\n    '.join(tb)), w
            return None, w


def mix_cfg(seed, mix=None):
    """A seed's configuration; the self-test mix starts on the engine under test (no upgrade)."""
    return dict(cfg_for(seed), mix=mix, upgrade=0) if mix else cfg_for(seed)


def run_seed(seed, steps=60, engine='current', checks=ALL_CHECKS, mix=None):
    cfg = mix_cfg(seed, mix)
    ops = gen_ops(seed, steps, cfg)
    v, _ = run_ops(seed, cfg, ops, engine, checks)
    return v, cfg, ops


def shrink(seed, cfg, ops, engine='current', checks=ALL_CHECKS, first=None):
    """Delta debugging: drop chunks of 8, 4, 2, 1 ops while the same violation still fires."""
    if first is None:
        first, _ = run_ops(seed, cfg, ops, engine, checks)
    if first is None:
        return ops, None, None
    cur = list(ops)
    changed = True
    while changed:
        changed = False
        for n in (8, 4, 2, 1):
            i = 0
            while i < len(cur):
                cand = cur[:i] + cur[i + n:]
                v, _ = run_ops(seed, cfg, cand, engine, checks)
                if v is not None and v.sig == first.sig:
                    cur, changed = cand, True
                else:
                    i += n
    v, w = run_ops(seed, cfg, cur, engine, checks)
    return cur, v, w


def report(seed, cfg, ops, v, w):
    lines = [f'seed {seed}: {v}', f'  literal: ({seed!r}, {cfg!r}, {ops!r})', '  trace:']
    lines += ['    ' + t for t in (w.trace if w else [])]
    return '\n'.join(lines)


def _parse_seeds(text):
    out = []
    for part in text.split(','):
        if '-' in part:
            lo, hi = (int(x) for x in part.split('-'))
            out.extend(range(lo, hi + 1))
        elif part:
            out.append(int(part))
    return out


def _worker(args):
    seeds, steps, engine, checks = args[:4]
    mix = args[4] if len(args) > 4 else None
    found = []
    for seed in seeds:
        v, _, _ = run_seed(seed, steps, engine, checks, mix)
        if v is not None:
            found.append((seed, v.inv, v.kind, str(v)))
    return len(seeds), found


def run_many(seeds, steps=60, engine='current', checks=ALL_CHECKS, jobs=1, mix=None):
    """Every failing seed as (seed, inv, kind, message)."""
    if jobs <= 1:
        return _worker((seeds, steps, engine, checks, mix))[1]
    import multiprocessing
    size = max(1, min(250, len(seeds) // (jobs * 4) or 1))
    blocks = [(seeds[i:i + size], steps, engine, checks, mix) for i in range(0, len(seeds), size)]
    found = []
    with multiprocessing.get_context('spawn').Pool(jobs) as pool:
        for _, f in pool.imap_unordered(_worker, blocks):
            found.extend(f)
    return sorted(found)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--engine', default='current')
    ap.add_argument('--seeds', default='0-499')
    ap.add_argument('--steps', type=int, default=60)
    ap.add_argument('--jobs', type=int, default=1)
    ap.add_argument('--shrink', action='store_true', help='shrink and print the first failures of each invariant')
    ap.add_argument('--checks', default='', help='comma-separated invariants to check (default: all)')
    ap.add_argument('--show', type=int, default=2, help='failures to print per invariant')
    ap.add_argument('--mix', default=None, help="'removals': the self-test's op mix (see gen_removal_mix)")
    a = ap.parse_args(argv)
    checks = ALL_CHECKS
    if a.checks:
        checks = frozenset(x if x in ('R', 'crash') else int(x) for x in a.checks.split(','))
    seeds = _parse_seeds(a.seeds)
    t0 = time.perf_counter()
    found = run_many(seeds, a.steps, a.engine, checks, a.jobs, a.mix)
    dt = time.perf_counter() - t0
    by_inv = collections.defaultdict(list)
    for seed, inv, kind, msg in found:
        by_inv[inv].append((seed, kind, msg))
    for inv in sorted(by_inv, key=str):
        for seed, kind, msg in by_inv[inv][:a.show]:
            if a.shrink:
                cfg = mix_cfg(seed, a.mix)
                ops = gen_ops(seed, a.steps, cfg)
                small, v, w = shrink(seed, cfg, ops, a.engine, checks)
                print(report(seed, cfg, small, v, w))
            else:
                print(f'seed {seed}: {msg}')
    kinds = collections.Counter((inv, kind) for _, inv, kind, _ in found)
    print(f'engine={a.engine} seeds={len(seeds)} steps={a.steps} failing={len(found)} '
          f'by invariant={ {str(k): len(v) for k, v in sorted(by_inv.items(), key=lambda kv: str(kv[0]))} } '
          f'kinds={dict(kinds.most_common(8))} time={dt:.1f}s ({dt / max(1, len(seeds)) * 1000:.0f} ms/seed)')
    return 1 if found else 0


if __name__ == '__main__':
    sys.exit(main())
