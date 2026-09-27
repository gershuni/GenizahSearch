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

Invariants (after every step unless stated):
  1  user text is never lost: every live note line and tag is in some copy
  2  a row changes list only by a desktop's orphan move; identities only fill
  3  nothing is deleted: no DELETE, no row vanishes during a sync, and a pass
     drops no local membership
  4  a row is named by one record; a desktop never inserts a row it should have
     claimed; the settle reaches a fixed point
  5  a second identical successful sync changes nothing
  6  My Library (97...) sys_ids never reach the cloud or come back from it
  7  records carry their account
  8  at the settle every membership has a record naming a row of its own list
  9  at the settle every cloud row reaches a local item that is the same entry
  R  rule checks on every request and around every pass (_check_request, _after_pass)

Nothing here talks to a network or writes a file: saves and snapshots stay in
memory, and time.time/uuid.uuid4 are seeded for the length of a run.
"""
import argparse
import collections
import copy
import importlib
import importlib.util
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
INJECTIONS = ('raise_before', 'raise_after', 'api_error', 'anon', 'session_lost', 'web', 'web',
              'other_desktop_pass', 'same_desktop_edit', 'url_too_long',
              'web_between_pages', 'web_between_pages', 'web_between_pages', 'web_between_pages')
# Result errors of a pass that returned before touching anything.
EARLY_ERRORS = ('Sync not available', 'Sync already in progress', 'No Supabase client')
IDENTITY_FIELDS = {'store': ('cloud_account',), 'projects': ('cloud_id',), 'lists': ('cloud_id',),
                   'items': ('cloud_id', 'cloud_rows')}
ALL_CHECKS = frozenset({1, 2, 3, 4, 5, 6, 7, 8, 9, 'R', 'crash'})

_NOWHERE = os.path.join(tempfile.gettempdir(), 'genizah-list-sync-scenarios-never-written')


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
    try:
        yield
    finally:
        time.time, uuid.uuid4 = real_time, real_uuid4
        logging.disable(prev_disable)
        for m, avail, key in saved:
            m.SUPABASE_AVAILABLE, m.SUPABASE_ANON_KEY = avail, key


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
        if action == 'api_23502':
            raise APIError({'code': '23502', 'message': 'null value in column violates not-null constraint'})
        if self.op in ('update', 'delete'):
            limit = db.gateway_limit if c.url_limit is None else min(db.gateway_limit, c.url_limit)
            if query_string_length(self.filters) > limit:
                raise APIError({'code': 414, 'message': '<html>414 Request-URI Too Large</html>'})
        out = self._run(user)
        if world is not None:
            world.after(c, self)
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
        self.user = USER
        self.client = FakeClient(world.db, name, USER)
        self.lm = self._manager(None)
        self.engine = engine
        self.sync = self._sync(engine)
        self.reidentified = set()  # items whose fl_id a user op set or changed (add_item on an existing key)

    def _manager(self, data):
        desk = self

        class ScenarioListsManager(lm_mod.ListsManager):
            LISTS_FILE = os.path.join(_NOWHERE, f'{desk.name}.pkl')

            def save(self):
                desk.disk = pickle.dumps(self.data)  # also proves the store stays picklable
                return True

            def write_snapshot(self, label):
                desk.snaps.append(copy.deepcopy(self.data))
                del desk.snaps[:-12]
                return True

        lm = ScenarioListsManager(None)
        if data is not None:
            lm.data = data
        return lm

    def _sync(self, engine):
        s = engine.ListsCloudSync(self.lm)
        s.set_client(self.client)
        s.set_user(self.user)
        return s

    def use_engine(self, engine):
        self.engine = engine
        self.sync = self._sync(engine)

    def restart(self):
        data = pickle.loads(self.disk) if self.disk is not None else None
        self.lm = self._manager(data)
        self.sync = self._sync(self.engine)

    def sign_in(self, user):
        self.user = user
        self.client.session_user = user
        self.sync.set_user(user)

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
        self.start_rows = {}
        self.pass_start_lists = {}
        self.pass_start_items = {}


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
        self.desks = {'A': Desk(self, 'A', first), 'B': Desk(self, 'B', first)}
        self.tok = 0
        self.live_n, self.live_g = set(), set()
        # retired note tokens a paste made live again, by the row they were pasted into: if that
        # row goes, they are retired again (the older copies were already being replaced)
        self.revived = {}
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
        c = self.ctx
        if (c is not None and c.inject and c.counting and not c.fired and client is c.desk.client):
            c.req_count += 1
            if c.inject[1] == 'web_between_pages':
                # the first later page of any list's read in the step
                if req.t == 'list_items' and req.op == 'select' and req.rng and req.rng[0] > 0:
                    c.fired = True
                    return self._fire(c, client, req)
            elif c.req_count == c.inject[0]:
                c.fired = True
                return self._fire(c, client, req)
        return None

    def after(self, client, req):
        return None

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
        if kind == 'web_between_pages':
            # The website removes rows the earlier pages returned, so the next page starts
            # later and the read skips rows it never returned: preferably up to a row this
            # desktop holds the entry of with no record (the one an insert would duplicate).
            lid = next((val for k, col, val in req.filters if k == 'eq' and col == 'list_id'), None)
            rows = sorted((r for r in self.db.tables['list_items'] if lid is not None and _eq(r['list_id'], lid)),
                          key=lambda r: r['id'])
            start = req.rng[0]
            hp = self.db.has_page and not client.page_missing
            data = c.desk.data
            local = next((k for k, ld in data['lists'].items() if _eq(ld.get('cloud_id'), lid)), None)
            named = {str(rec.get('id')) for it in data['items'].values() for _, rec in _records(it)}

            def unrecorded(r):
                return local is not None and str(r['id']) not in named and any(
                    local in (it.get('lists') or []) and not (it.get('cloud_rows') or {}).get(local)
                    and same_entry(ident(iid, it), row_ident(r, hp), hp) for iid, it in data['items'].items())

            t = next((t for t in range(start, len(rows)) if unrecorded(rows[t])), start)
            for r in rows[:max(1, min(t - start + 1, start))]:
                self.web_remove(r)
            return None
        if kind == 'other_desktop_pass':
            other = self.desks['B' if c.desk.name == 'A' else 'A']
            direction = 'up' if sel[0] % 2 else 'down'
            other.client.page_missing = False
            res = other.sync.sync_to_cloud() if direction == 'up' else other.sync.sync_from_cloud()
            self.trace.append(f'    {other.name}: {direction} (while {c.desk.name} syncs) -> {_summary(res)}')
            return None
        if kind == 'same_desktop_edit':
            d = c.desk
            before = self.memberships(d)
            self.desk_op(d, sel, during_pass=True)
            c.same_edit_removed |= before - self.memberships(d)
            c.same_edit_fired = True
            return None
        raise AssertionError(kind)

    def _check_request(self, d, client, req):
        pls = req.payloads()
        name = d.name
        if req.op == 'delete':
            self.pend(3, 'delete', f'{name} sent a DELETE on {req.t} {req.filters}')
        for p in pls:
            if str(p.get('sys_id') or '').startswith('97'):
                self.pend(6, 'local-sent', f'{name} sent My Library sys_id {p.get("sys_id")} to {req.t}')
        for kind, col, val in req.filters:
            vals = val if isinstance(val, list) else [val]
            if col == 'sys_id' and any(str(v).startswith('97') for v in vals):
                self.pend(6, 'local-sent', f'{name} filtered {req.t} on My Library sys_id {val}')
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
        for lid, ld in d.data.get('lists', {}).items():
            if ld.get('cloud_id') is not None:
                owners[str(ld['cloud_id'])].add(lid)
        c = self.ctx
        lenient = c is not None and c.same_edit_fired
        items = dict(d.data.get('items', {}))
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
                if in_dest and not in_src:
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
        for r in self.db.visible('list_items', d.client.session_user):
            if not _eq(r['list_id'], target) or not _eq(c.start_rows.get(r['id']), target):
                continue
            if not same_entry(e, row_ident(r, hp), hp):
                continue
            if any(self._names_row(it, r['id'], include_gone=True) for it in d.data.get('items', {}).values()):
                continue
            self.pend(4, 'insert-unclaimed', f'{d.name} inserted {e} into cloud list {target}, which already held '
                                             f'row {r["id"]} {row_ident(r, hp)} that no record of {d.name} names')

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

    def web_op(self, sel):
        kind = sel[0] % 9
        rows = sorted((r for r in self.db.tables['list_items'] if self.db.owner('list_items', r) == USER),
                      key=lambda r: r['id'])
        wl = self.web_lists()
        if kind in (0, 1) and wl:                        # add_list_item (may duplicate an entry)
            lst = wl[sel[1] % len(wl)]
            sys_id = SYS[sel[2] % len(SYS)]
            fl = (None, 'FLa', 'FLb')[sel[3] % 3]
            note = self.fresh('n') if sel[4] % 2 else ''
            if note:
                self.live_n.add(note)
            self.web.table('list_items').insert({'list_id': lst['id'], 'sys_id': sys_id, 'shelfmark': None,
                                                 'title': None, 'fl_id': fl, 'note': note, 'tags': []}).execute()
            self.trace.append(f'web: add ({sys_id},{fl}) note={note!r} to cloud list {lst["id"]} {lst["name"]}')
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

    # ---- desktop user ops
    def user_lists(self, d):
        return [lid for lid, lst in d.data['lists'].items() if lid != 'recent' and not lst.get('is_system')]

    def desk_op(self, d, sel, during_pass=False):
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
                if sel[2] % 2:
                    r = lm.auto_merge_duplicate_group(g)
                    self.trace.append(f'{tag}: clean up duplicates of {g["name"]} (auto), keep {r.get("keep_id")}')
                else:
                    ids = [x['id'] for x in g['lists']]
                    keep = ids[sel[3] % len(ids)]
                    lm.merge_duplicate_group(keep, [i for i in ids if i != keep])
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
            lm.save()
            self.trace.append(f'{tag}: restore lists.pkl from its snapshot #{k}')
            self.retire_if_lost()
        elif kind == 11:                                 # empty the Trash
            n = lm.empty_trash()
            if n:
                self.trace.append(f'{tag}: empty the Trash ({n} list(s))')
                self.retire_if_lost()

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

    def check_account(self, d, where, before_gone=None):
        store = d.data
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

    # ---- one pass, with the checks that belong around it
    def one_pass(self, d, direction):
        d.client.page_missing = False
        store0 = copy.deepcopy(d.data)
        c = self.ctx
        if c is not None:
            c.pass_start_lists = {iid: list(it.get('lists', [])) for iid, it in d.data['items'].items()}
            c.pass_start_items = copy.deepcopy(d.data['items'])
            edits_before = c.same_edit_fired
        # a pass as another account than the one the store's records belong to
        switched = d.data.get('cloud_account') not in (None, d.user)
        gone0 = {(iid, k) for iid, it in d.data.get('items', {}).items()
                 for k, rec in _records(it) if rec.get('gone')} if switched else None
        res = d.sync.sync_to_cloud() if direction == 'up' else d.sync.sync_from_cloud()
        if not isinstance(res, dict):
            raise Violation('crash', 'bad-result', f'{d.name} {direction} returned {res!r}')
        edited = c is not None and c.same_edit_fired and not edits_before
        reached = res.get('error') not in EARLY_ERRORS
        if self.checking:
            if direction == 'up' and not edited and d.data is not None:
                fields = getattr(d.engine, 'IDENTITY_FIELDS', IDENTITY_FIELDS)
                a, b = _without_identity(store0, fields), _without_identity(d.data, fields)
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
        if reached and switched:
            if self.checking:
                self.flush(f'{d.name} {direction}')
                self.check_account(d, f'{d.name} {direction} after an account switch', gone0)
        return res

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
        kind = op[0]
        where = f'step {i}'
        if kind == 'web':
            self.web_op(op[1])
            self.check_tokens(f'{where} web')
            return
        if kind == 'migrate':
            if self.db.migrate():
                self.trace.append('web: the page column is added' + (' (schema cache lags)'
                                                                     if self.db.page_lag else ''))
            return
        d = self.desks[op[1]]
        if kind == 'desk':
            self.desk_op(d, op[2])
            self.check_tokens(f'{where} {d.name} user op')
            return
        if kind == 'long':
            self.long_note(d, op[2])
            return
        if kind == 'restart':
            d.restart()
            self.trace.append(f'{d.name}: restart (lists.pkl as last saved)')
            self.retire_if_lost()
            return
        if kind == 'account':
            if d.user != op[2]:
                d.sign_in(op[2])
                self.trace.append(f'{d.name}: sign in as {op[2]}')
            return
        assert kind == 'sync', op
        direction, inj = op[2], op[3]
        where = f'{where} {d.name} {direction}'
        c = _Step(d, tuple(inj) if inj else None)
        c.start_rows = {r['id']: r['list_id'] for r in self.db.tables['list_items']}
        before_m = self.memberships(d)
        self.db.lagged = False
        self.ctx = c
        c.counting = True
        try:
            res = self.run_sync(d, direction)
        finally:
            c.counting = False
            self.ctx = None
            d.client.session_user = d.user
            d.client.url_limit = None
        self.trace.append(f'{d.name}: {direction}' + (f' inject={inj[:2]} fired={c.fired}' if inj else '')
                          + ' -> ' + '; '.join(_summary(r) for r in res))
        if not self.checking:
            self.pending = []
            return
        self.flush(where)
        after_m = self.memberships(d)
        lists_now = d.data.get('lists', {})
        for (lid, idn) in sorted(before_m - c.same_edit_removed, key=repr):
            if lid in lists_now and not any(l2 == lid and compatible(idn, i2) for (l2, i2) in after_m):
                self.viol(3, 'membership-dropped', f'{where}: entry {idn} left local list {lid} during a sync pass')
        vanished = set(c.start_rows) - {r['id'] for r in self.db.tables['list_items']} - c.web_deleted
        if vanished:
            self.viol(3, 'row-vanished', f'{where}: cloud rows {sorted(vanished)} disappeared during a sync pass')
        self.check_tokens(where)
        for dd in self.desks.values():
            self.check_claims(dd, where)
            self.check_account(dd, where)
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
        for d in self.desks.values():
            if d.user != USER:
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
                return
            prev = st
        self.viol(4, 'no-fixed-point', f'no fixed point after {rounds} settle rounds: {_diff(*self._one_more_round())}')

    def _one_more_round(self):
        a = self.state()
        for name, direction in (('A', 'up'), ('B', 'up'), ('A', 'down'), ('B', 'down')):
            self.one_pass(self.desks[name], direction)
        return a, self.state()

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

            def local_name(cl):
                return {'General': lists.get('default', {}).get('name')}.get(cl['name'], cl['name'])

            for cl in cloud:
                if cl.get('deleted_at') or cl['name'] == 'Recently Viewed':
                    continue
                lid = owned.get(str(cl['id']))
                if lid is None:   # a same-name cloud list is read for the first local list of its name
                    lid = next((x for x in order if x != 'recent' and not lists[x].get('is_system')
                                and lists[x].get('name') == local_name(cl)), None)
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
            'notes_too_long', 'notes_differing', 'unchecked', 'waiting', 'complete')
    parts = [f'{k}={r[k]}' for k in keys if k in r and r[k] not in (0, None, [])]
    if r.get('web_removed'):
        parts.append(f'web_removed={len(r["web_removed"])}')
    if r.get('error'):
        parts.append(f'error={str(r["error"])[:60]!r}')
    return ' '.join(parts)


# --------------------------------------------------------------------------- generation, runs, shrinking

def cfg_for(seed):
    rng = random.Random(seed * 31 + 7)
    return {'has_page': rng.random() < 0.8, 'max_rows': rng.choice([None, None, 2, 3]),
            'p_inject': rng.choice([0.0, 0.1, 0.25]), 'past_end_raises': rng.random() < 0.5,
            'page_lag': rng.random() < 0.25,
            'upgrade': rng.randrange(10, 21) if rng.random() < 0.2 else 0}


def gen_ops(seed, steps, cfg):
    rng = random.Random(seed * 7919 + 1)
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
            else:
                ops.append(('desk', desk, sel))
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


def run_seed(seed, steps=60, engine='current', checks=ALL_CHECKS):
    cfg = cfg_for(seed)
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
    seeds, steps, engine, checks = args
    found = []
    for seed in seeds:
        v, _, _ = run_seed(seed, steps, engine, checks)
        if v is not None:
            found.append((seed, v.inv, v.kind, str(v)))
    return len(seeds), found


def run_many(seeds, steps=60, engine='current', checks=ALL_CHECKS, jobs=1):
    """Every failing seed as (seed, inv, kind, message)."""
    if jobs <= 1:
        return _worker((seeds, steps, engine, checks))[1]
    import multiprocessing
    size = max(1, min(250, len(seeds) // (jobs * 4) or 1))
    blocks = [(seeds[i:i + size], steps, engine, checks) for i in range(0, len(seeds), size)]
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
    a = ap.parse_args(argv)
    checks = ALL_CHECKS
    if a.checks:
        checks = frozenset(x if x in ('R', 'crash') else int(x) for x in a.checks.split(','))
    seeds = _parse_seeds(a.seeds)
    t0 = time.perf_counter()
    found = run_many(seeds, a.steps, a.engine, checks, a.jobs)
    dt = time.perf_counter() - t0
    by_inv = collections.defaultdict(list)
    for seed, inv, kind, msg in found:
        by_inv[inv].append((seed, kind, msg))
    for inv in sorted(by_inv, key=str):
        for seed, kind, msg in by_inv[inv][:a.show]:
            if a.shrink:
                cfg = cfg_for(seed)
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
