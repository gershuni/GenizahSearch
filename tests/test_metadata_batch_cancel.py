"""A cancelled batch_fetch_shelfmarks drops the requests still queued (Codex review of PR #376).

It submits every id to the shared executor up front; a cancel only stopped reading the
results, so the queued requests still ran and were thrown away, while the desktop's next
fetch asked for the same ids again (start_metadata_loading carries the ids a replaced
fetch did not reach).
"""
import threading
from concurrent.futures import ThreadPoolExecutor

from shared.metadata_manager import MetadataManager


def test_a_cancelled_batch_runs_no_queued_request():
    mgr = MetadataManager.__new__(MetadataManager)     # no index dir, CSV or caches
    mgr.nli_cache, mgr.csv_bank = {}, {}
    mgr.nli_executor = ThreadPoolExecutor(max_workers=1)
    mgr.save_caches = lambda: None
    cancelled, release, ran = [False], threading.Event(), []

    def fetch(sid):
        ran.append(sid)
        if sid == "1":
            cancelled[0] = True          # the cancel arrives while "1" is in flight
        elif sid == "2":
            release.wait(5)              # holds the one worker until the batch returned
        return sid, {"shelfmark": sid, "title": ""}

    mgr._fetch_single_worker = fetch
    try:
        mgr.batch_fetch_shelfmarks(["1", "2", "3", "4"], check_cancel=lambda: cancelled[0])
    finally:
        release.set()
        mgr.nli_executor.shutdown(wait=True)
    assert "3" not in ran and "4" not in ran, ran
    assert "3" not in mgr.nli_cache


def test_an_uncancelled_batch_fetches_every_id():
    mgr = MetadataManager.__new__(MetadataManager)
    mgr.nli_cache, mgr.csv_bank = {}, {}
    mgr.nli_executor = ThreadPoolExecutor(max_workers=2)
    mgr.save_caches = lambda: None
    mgr._fetch_single_worker = lambda sid: (sid, {"shelfmark": sid, "title": ""})
    try:
        mgr.batch_fetch_shelfmarks(["1", "2", "3"], check_cancel=lambda: False)
    finally:
        mgr.nli_executor.shutdown(wait=True)
    assert all(s in mgr.nli_cache for s in ("1", "2", "3"))
