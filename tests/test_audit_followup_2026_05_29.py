# -*- coding: utf-8 -*-
"""Regression tests for the 2026-05-29 codebase-audit follow-up fixes.

Three independent silent/crash-class bugs surfaced by the fan-out audit:

1. lists_sync.sync_to_cloud reported success=True and an inflated items_pushed
   count even when every cloud insert was silently swallowed (false success /
   masked data loss).
2. genizah_core MetadataManager._fetch_single_worker returned a BARE dict for
   synthetic sys_ids instead of the (system_id, meta) 2-tuple every caller
   unpacks -> ValueError: too many values to unpack.
3. shared.puzzle_service.PuzzleService.save_document left a dirty open
   transaction on failure (no rollback); the next successful commit would
   flush the partial writes, desyncing join_document_fragments.
"""

from shared.puzzle_model import PuzzleDocument, PuzzleFragment


# ---------------------------------------------------------------------------
# Fix 1 — lists_sync false-success reporting
# ---------------------------------------------------------------------------
class _FakeResp:
    def __init__(self, data, count=None):
        self.data = data
        self.count = count  # rows the select's filters match (none: this cloud starts empty)


class _FakeQuery:
    def __init__(self, table, client):
        self._table = table
        self._client = client
        self._op = None
        self._payload = None

    def select(self, *a, **k):
        self._op = 'select'
        return self

    def insert(self, payload):
        self._op = 'insert'
        self._payload = payload
        return self

    def update(self, payload):
        self._op = 'update'
        self._payload = payload
        return self

    def eq(self, *a, **k):
        return self

    # The sync pages its reads by row id (order + limit, gt after the first page)
    # and filters conditional writes; this cloud holds nothing to filter.
    order = limit = gt = in_ = is_ = contains = contained_by = eq

    def execute(self):
        return self._client._execute(self._table, self._op, self._payload)


class _FakeClient:
    """Minimal supabase-py stand-in. Optionally fails list_items inserts."""

    def __init__(self, fail_list_items_insert=False):
        self.fail_list_items_insert = fail_list_items_insert
        self._counter = 0

    def table(self, name):
        return _FakeQuery(name, self)

    def _next_id(self):
        self._counter += 1
        return f"cloud-{self._counter}"

    def _execute(self, table, op, payload):
        if op == 'select':
            return _FakeResp([], count=0)
        if op == 'update':
            return _FakeResp([])
        if op == 'insert':
            if table == 'list_items' and self.fail_list_items_insert:
                raise RuntimeError("simulated list_items insert failure (RLS denial)")
            # PostgREST returns the full inserted rows
            if isinstance(payload, list):
                return _FakeResp([dict(p, id=self._next_id()) for p in payload])
            return _FakeResp([dict(payload, id=self._next_id())])
        return _FakeResp([])


class _FakeListsManager:
    def __init__(self):
        self.data = {
            'projects': {},
            'lists': {'L1': {'name': 'My List'}},
            'items': {'I1': {'sys_id': 'S1', 'lists': ['L1'], 'note': 'hi', 'tags': []}},
        }

    def save(self):
        pass


def _make_sync(monkeypatch, fail_list_items_insert=False, manager=None):
    from shared import lists_sync
    monkeypatch.setattr(lists_sync, 'SUPABASE_AVAILABLE', True)
    monkeypatch.setattr(lists_sync, 'SUPABASE_ANON_KEY', 'test-key')
    sync = lists_sync.ListsCloudSync(manager or _FakeListsManager())
    sync.set_user('user-1')
    sync.set_client(_FakeClient(fail_list_items_insert))
    return sync


class TestListsSyncCounting:
    def test_clean_sync_reports_success_and_real_count(self, monkeypatch):
        sync = _make_sync(monkeypatch, fail_list_items_insert=False)
        result = sync.sync_to_cloud()
        assert result['success'] is True
        assert result['items_pushed'] == 1
        assert result['items_failed'] == 0

    def test_failed_item_insert_is_not_reported_as_success(self, monkeypatch):
        sync = _make_sync(monkeypatch, fail_list_items_insert=True)
        result = sync.sync_to_cloud()
        # The list row still creates, but the single item fails to persist.
        # Before the fix: success=True, items_pushed=1 (false success).
        assert result['success'] is False
        assert result['items_pushed'] == 0
        assert result['items_failed'] == 1
        assert 'failed to upload' in (result.get('error') or '')

    # "An update that matches no row is not counted" moved to
    # tests/test_list_sync_identity.py: a legacy cloud_id is no longer used
    # to find a row, so its premise -- a stale id routed to an update -- is gone.


# ---------------------------------------------------------------------------
# Fix 2 — _fetch_single_worker synthetic sys_id return shape
# ---------------------------------------------------------------------------
class TestFetchSingleWorkerSynthetic:
    def test_synthetic_sys_id_returns_two_tuple(self):
        from genizah_core import MetadataManager
        from shared.synthetic_sys_id import encode_inventory_sys_id

        mm = MetadataManager.__new__(MetadataManager)  # skip __init__ side effects
        synthetic = encode_inventory_sys_id(123456)

        result = mm._fetch_single_worker(synthetic)
        # The unpack itself is the regression assertion: the pre-fix bare-dict
        # return raised "ValueError: too many values to unpack (expected 2)".
        sid, meta = result
        assert sid == synthetic
        assert isinstance(meta, dict)
        assert meta['fl_ids'] == []


# ---------------------------------------------------------------------------
# Fix 3 — PuzzleService.save_document rolls back on failure
# ---------------------------------------------------------------------------
class TestPuzzleSaveRollback:
    def test_failed_save_is_rolled_back_not_flushed_by_next_commit(self, tmp_path):
        from shared.puzzle_service import PuzzleService

        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))

        d1 = PuzzleDocument(
            title="D1", fragments=[PuzzleFragment(sys_id="A", folio_label="1r", fl_id="FLA")]
        )
        assert svc.save_document(d1) == d1.id

        # D2 fails mid-transaction: a NOT NULL violation on the fragment insert
        # fires AFTER the join_documents INSERT OR REPLACE + DELETE are pending.
        d2 = PuzzleDocument(
            title="D2", fragments=[PuzzleFragment(sys_id="B", folio_label="1r", fl_id=None)]
        )
        assert svc.save_document(d2) is None  # failure path

        # D3 commits cleanly. Without the rollback, this commit would flush D2's
        # orphaned join_documents row.
        d3 = PuzzleDocument(
            title="D3", fragments=[PuzzleFragment(sys_id="C", folio_label="1r", fl_id="FLC")]
        )
        assert svc.save_document(d3) == d3.id

        assert svc.load_document(d2.id) is None  # rolled back, never persisted
        assert svc.load_document(d1.id) is not None
        assert svc.load_document(d3.id) is not None

    def test_save_document_has_no_lingering_open_transaction(self, tmp_path):
        """After a failed save the connection must not hold a dirty transaction."""
        from shared.puzzle_service import PuzzleService

        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        bad = PuzzleDocument(
            title="bad", fragments=[PuzzleFragment(sys_id="X", folio_label="1r", fl_id=None)]
        )
        assert svc.save_document(bad) is None
        # in_transaction is False only if commit/rollback settled the txn.
        assert svc._conn.in_transaction is False
