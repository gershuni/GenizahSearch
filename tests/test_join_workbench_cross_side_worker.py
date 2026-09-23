# -*- coding: utf-8 -*-
"""Desktop Joins Lab "other side of the leaf" -- the REAL _CrossSideWorker call site.

The worker composed the other side's query string, then handed apply_cross_side() the
SideQuery object instead. SearchEngine.execute_search runs strip_search_diacritics() on
its query, so that raised TypeError, and _DesktopSearchExecutor's `except Exception:
return []` swallowed it: on desktop, Narrow always emptied the results and Widen never
added a page. The contract tests could not see it -- their fake executor accepts any
object. The executor here rejects non-strings the way the real engine does.
"""
import pytest

pytestmark = pytest.mark.gui  # imports PyQt6 (QThread): gui bucket only

from shared.joins_lab import BuilderRow, SideQuery, normalize_candidate  # noqa: E402
from desktop.join_workbench import _CrossSideWorker  # noqa: E402

SID = "990000000010205171"


def _res(page):
    return {
        "display": {"id": SID, "shelfmark": "T-S 1", "title": "", "library_code": "CUL",
                    "img": page},
        "uid": f"{SID}_P{page:04d}",
        "full_text": "",
    }


class _StrictExec:
    """Behaves like SearchEngine: the query must be a string."""

    def __init__(self, b_results):
        self.b_results = b_results
        self.calls = []

    def execute_search(self, query_str, mode, gap, **kwargs):
        if not isinstance(query_str, str):
            raise TypeError(f"expected string, got {type(query_str).__name__}")
        self.calls.append((query_str, kwargs))
        return list(self.b_results)

    def get_browse_page(self, sys_id, p_num=None, **kwargs):
        return {"total_pages": 10, "text": "", "p_num": p_num}


def _run(worker):
    got = []
    worker.done.connect(got.append)
    worker.run()  # synchronously, on this thread: the call site under test
    return got


def _worker(executor, combine, b_term="אבגד"):
    base = [normalize_candidate(_res(3))]
    b_side = SideQuery(rows=(BuilderRow(term=b_term),))
    return _CrossSideWorker(executor, base, b_side, {}, combine, None)


def test_narrow_keeps_base_page_whose_neighbour_matches_side_b():
    ex = _StrictExec([_res(4)])            # B matches page 4; base page is 3 -> neighbours
    got = _run(_worker(ex, "AND"))
    assert ex.calls, "side B was never searched"
    assert isinstance(ex.calls[0][0], str) and "אבגד" in ex.calls[0][0]
    assert len(got) == 1
    assert [c.page for c in got[0].candidates] == [3]


def test_narrow_drops_base_page_without_matching_neighbour():
    ex = _StrictExec([_res(9)])
    got = _run(_worker(ex, "AND"))
    assert len(got) == 1 and list(got[0].candidates) == []


def test_widen_adds_the_neighbour_of_a_side_b_hit():
    ex = _StrictExec([_res(8)])
    got = _run(_worker(ex, "OR"))
    pages = sorted(c.page for c in got[0].candidates)
    assert 3 in pages and (7 in pages or 9 in pages), pages


def test_side_b_text_position_is_forwarded():
    ex = _StrictExec([])
    _run(_worker(ex, "AND"))
    assert "text_position" in ex.calls[0][1]


def test_cancelled_worker_does_not_emit():
    ex = _StrictExec([_res(4)])
    w = _worker(ex, "AND")
    w.cancel()
    assert _run(w) == []


# --- crash-safe replacement: a running worker must be retained, not dropped ----------
# Before the fix above the worker died within microseconds, so replacing it could not
# destroy a running QThread. Now it runs a real engine search, and dropping the only
# reference mid-run aborts the process on Windows (0xC0000409) -- the _EnrichWorker bug.

class _Sig:
    def __init__(self, name, log):
        self._name, self._log = name, log

    def connect(self, _slot):
        self._log.append(self._name + ":connect")

    def disconnect(self, _slot=None):
        self._log.append(self._name + ":disconnect")


class _FakeCrossWorker:
    def __init__(self, log, running):
        self._log, self._running = log, running
        self.done = _Sig("done", log)
        self.finished = _Sig("finished", log)

    def cancel(self):
        self._log.append("cancel")

    def isRunning(self):
        return self._running


def _pane_stub(worker):
    from desktop.join_workbench import JoinCandidatePane

    class _Stub:
        _reap_enrich_worker = JoinCandidatePane._reap_enrich_worker

    s = _Stub()
    s._cross_worker = worker
    s._cross_gen = 0
    s._retired_workers = []
    s._on_cross_done = lambda r: None
    return s


@pytest.mark.parametrize("running", [True, False])
def test_retire_cross_worker(running):
    from desktop.join_workbench import JoinCandidatePane

    log = []
    worker = _FakeCrossWorker(log, running)
    stub = _pane_stub(worker)
    JoinCandidatePane._retire_cross_worker(stub)
    assert stub._cross_worker is None
    assert "cancel" in log and "done:disconnect" in log
    if running:
        assert stub._retired_workers == [worker]
        assert "finished:connect" in log
    else:
        assert stub._retired_workers == []


def test_search_start_retires_the_old_cross_worker():
    """The call site: starting a cross-side search goes through _retire_cross_worker."""
    import inspect
    from desktop.join_workbench import JoinCandidatePane

    src = inspect.getsource(JoinCandidatePane)
    start = src.index("self._cross_worker = _CrossSideWorker(")
    assert "self._retire_cross_worker()" in src[max(0, start - 300):start]


# --- Codex P1 on #359: a superseded worker must never overwrite a newer search -------
# Retiring only when the NEXT search also started a cross-side worker left a hole: clear
# or disable the other side, search again, and the old worker's late result replaced the
# new search's candidates.

def test_stale_cross_result_is_dropped():
    from desktop.join_workbench import JoinCandidatePane
    from shared.joins_lab import MergeResult

    class _Stub:
        _cross_gen = 5
        _text_cands = ["new"]
        assembled = 0

        def _maybe_assemble(self):
            self.assembled += 1

    s = _Stub()
    JoinCandidatePane._on_cross_done(s, MergeResult(candidates=("old",), note=""), 4)
    assert s._text_cands == ["new"] and s.assembled == 0
    JoinCandidatePane._on_cross_done(s, MergeResult(candidates=("cur",), note=""), 5)
    assert s._text_cands == ["cur"] and s.assembled == 1


@pytest.mark.parametrize("method", ["do_search", "_stop_search"])
def test_every_generation_bump_retires_the_cross_worker(method):
    """Each place that supersedes a search (new search; Stop that kills the thread) must
    retire the cross-side worker unconditionally, not only when a new one is started."""
    import ast
    import inspect
    import textwrap
    from desktop.join_workbench import JoinCandidatePane

    fn = ast.parse(textwrap.dedent(inspect.getsource(getattr(JoinCandidatePane, method))))
    calls = {
        n.func.attr for n in ast.walk(fn)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    }
    assert "_retire_cross_worker" in calls, method


def test_worker_result_is_connected_with_its_generation():
    import inspect
    from desktop.join_workbench import JoinCandidatePane

    src = inspect.getsource(JoinCandidatePane._on_results)
    assert "self._on_cross_done(res, g)" in src


# --- Codex round 2 on #359 -------------------------------------------------------------

def test_retire_advances_the_generation_so_a_queued_result_is_dropped():
    """A result already queued when the worker is retired must not land (a disconnect
    does not recall an event Qt has already posted)."""
    from desktop.join_workbench import JoinCandidatePane
    from shared.joins_lab import MergeResult

    stub = _pane_stub(_FakeCrossWorker([], running=False))
    stub._text_cands = ["new"]
    stub._maybe_assemble = lambda: None
    started_under = stub._cross_gen
    JoinCandidatePane._retire_cross_worker(stub)
    JoinCandidatePane._on_cross_done(
        stub, MergeResult(candidates=("old",), note=""), started_under)
    assert stub._text_cands == ["new"]


def test_reanchoring_retires_the_cross_worker():
    """P1: a re-anchor must invalidate an other-side search computed for the old anchor."""
    import ast
    import inspect
    import textwrap
    from desktop import join_workbench

    win = next(v for k, v in vars(join_workbench).items()
               if isinstance(v, type) and hasattr(v, "set_anchor")
               and hasattr(v, "_start_anchor_load"))
    fn = ast.parse(textwrap.dedent(inspect.getsource(win.set_anchor)))
    calls = {n.func.attr for n in ast.walk(fn)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert "_retire_cross_worker" in calls


def test_cancel_stops_the_underlying_engine_scan():
    """P2: the engine only stops when its progress_callback raises InterruptedError."""
    ex = _StrictExec([_res(4)])
    w = _worker(ex, "AND")
    _run(w)
    cb = ex.calls[0][1].get("progress_callback")
    assert callable(cb), "no progress_callback reached the engine"
    cb(1, 10)                      # not cancelled: harmless
    w.cancel()
    with pytest.raises(InterruptedError):
        cb(2, 10)


# --- Codex round 3 on #359: closing the lab must stop the scan ------------------------

def test_shutdown_retires_and_waits_for_a_running_cross_worker():
    from desktop.join_workbench import JoinCandidatePane

    log = []

    class _Waitable(_FakeCrossWorker):
        def wait(self, ms):
            log.append(f"wait:{ms}")
            self._running = False
            return True

    worker = _Waitable(log, running=True)
    stub = _pane_stub(worker)
    stub._retire_cross_worker = lambda: JoinCandidatePane._retire_cross_worker(stub)
    JoinCandidatePane.shutdown_background_workers(stub, timeout_ms=1234)
    assert "cancel" in log, "the scan was not cancelled"
    assert "wait:1234" in log, "close did not wait for the running QThread"
    assert stub._cross_worker is None


def test_workbench_close_shuts_down_the_candidate_pane():
    import ast
    import inspect
    import textwrap
    from desktop.join_workbench import JoinWorkbenchWindow

    fn = ast.parse(textwrap.dedent(inspect.getsource(JoinWorkbenchWindow.closeEvent)))
    calls = {n.func.attr for n in ast.walk(fn)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert "shutdown_background_workers" in calls
