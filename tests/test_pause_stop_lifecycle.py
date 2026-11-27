"""Source guards on the stop lifecycle.

Cancellation is checkpoint-only, so a PARKED worker never reaches the code that
reads cancel_flag. Every stop path therefore has to go through request_cancel(),
which sets the flag AND un-parks in one call. A path that only assigns the flag
would burn its whole wait() budget and fall through to QThread.terminate() —
on a thread holding a Tantivy searcher or a SQLite handle.

That invariant is invisible at runtime until it bites, so it is pinned here.

The Search tab's stop paths (Stop, New, Close) go through desktop/search_run.py
(GS-14), which calls request_cancel() and never waits or terminates; its
behaviour is tested in tests/test_search_run.py.
"""

import inspect

import pytest

import genizah_app as app

STOP_PATHS = [
    'stop_search',
    '_reset_search',
    'toggle_composition',
    'cancel_composition',
    '_reset_composition',
    'closeEvent',
]
# Their search worker goes through SearchRun (request_cancel() is called there).
SEARCH_STOP_PATHS = {'stop_search': 'stop', '_reset_search': 'discard', 'closeEvent': 'discard'}
# New on the Composition tab goes through its own holder (GS-14, PR 5).
COMP_STOP_PATHS = {'_reset_composition': 'run.discard(', 'closeEvent': 'comp_run.discard(',
                   '_drain_previous_worker': 'run.discard(', 'start_grouping': 'run.discard('}


@pytest.mark.parametrize('method', [m for m in STOP_PATHS
                                    if m not in SEARCH_STOP_PATHS and m not in COMP_STOP_PATHS])
def test_stop_path_uses_request_cancel(method):
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert 'request_cancel()' in src, (
        '%s does not call request_cancel(); a paused worker would stay parked '
        'until the wait() budget expired and terminate() fired' % method)


@pytest.mark.parametrize('method, call', sorted(SEARCH_STOP_PATHS.items()))
def test_search_stop_paths_go_through_search_run(method, call):
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert 'search_run_of(self).%s(' % call in src, method


def test_nothing_waits_on_or_terminates_the_search_worker():
    """GS-14: a search worker is never waited for on the UI thread nor terminated."""
    src = inspect.getsource(app)
    for bad in ('search_thread.wait(', 'search_thread.terminate('):
        assert bad not in src, bad


@pytest.mark.parametrize('method, call', sorted(COMP_STOP_PATHS.items()))
def test_composition_stop_paths_go_through_its_holder(method, call):
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert 'comp_run_of(self)' in src and call in src, method


def test_nothing_waits_on_or_terminates_a_composition_or_grouping_worker():
    """GS-14, PR 5: the scan and the grouping are never waited for on the UI thread nor
    terminated, and nothing in the window terminates a thread."""
    src = inspect.getsource(app)
    for bad in ('comp_thread.wait(', 'group_thread.wait(', '.terminate()'):
        assert bad not in src, bad


@pytest.mark.parametrize('method', STOP_PATHS)
def test_stop_path_does_not_assign_cancel_flag_directly(method):
    """A bare assignment is the exact bug request_cancel() exists to prevent."""
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert 'cancel_flag = True' not in src, (
        '%s assigns cancel_flag directly instead of calling request_cancel()' % method)


def test_no_direct_cancel_flag_assignment_remains_anywhere():
    src = inspect.getsource(app)
    assert 'cancel_flag = True' not in src


@pytest.mark.parametrize('method', ['stop_search', '_reset_search'])
def test_search_stop_preserves_the_cancelled_flag_ordering(method):
    """CR-114-02: _search_was_cancelled must be set BEFORE the cancel, so a
    cooperative on_search_finished sees it."""
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert src.index('_search_was_cancelled = True') < src.index('search_run_of(self).')


@pytest.mark.parametrize('method', STOP_PATHS[:-1])   # closeEvent has no UI left to paint
def test_stop_path_hides_the_pause_button(method):
    """Otherwise a Pause button sits visible over a run that is cancelling."""
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert '_apply_pause_state' in src and "'hidden'" in src, method


@pytest.mark.parametrize('method, guard', [('start_search', '_free_search_slot'),
                                           ('_complete_chain_then', '_free_search_slot'),
                                           ('run_composition', '_drain_previous_worker')])
def test_run_entry_points_refuse_to_rebind_a_live_worker(method, guard):
    """A paused worker lives indefinitely, and rebinding the slot over a live
    QThread drops its last reference — "QThread: Destroyed while thread is still
    running", a hard crash. The Search tab's slot is freed by holding the old
    worker until it ends (GS-14)."""
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert guard in src, method


def test_drain_refuses_rather_than_rebinding_a_third_run():
    """Two stopped workers still finishing and a third run would pile workers on the
    index: the drain refuses (GS-14, PR 5; behaviour in tests/test_comp_run.py)."""
    src = inspect.getsource(app.GenizahGUI._drain_previous_worker)
    assert 'return False' in src
    assert 'can_start(' in src


@pytest.mark.parametrize('method', ['start_search', 'run_composition'])
def test_run_entry_points_stamp_a_fresh_run_id(method):
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert '_run_seq' in src and 'run_id' in src, method


def test_grouping_hides_the_pause_button():
    """GroupingThread is not pausable. Showing a greyed-out Pause beside a live
    Stop would claim a capability that does not exist, at exactly the slow tail
    where the user most wants it."""
    src = inspect.getsource(app.GenizahGUI.start_grouping)
    assert '_apply_pause_state' in src and "'hidden'" in src


def test_close_event_no_longer_relies_on_the_no_op_interruption():
    """closeEvent called comp_thread.requestInterruption(), which the composition
    threads never polled. The mixin now routes that override into request_cancel,
    but closeEvent should be reaching it through the shared entry point: for the
    search worker that is SearchRun.discard (GS-14), which calls request_cancel()."""
    src = inspect.getsource(app.GenizahGUI.closeEvent)
    assert 'search_run_of(self).discard(' in src and 'comp_run.discard(' in src
    import desktop.search_run as search_run
    assert '_ask_to_stop(current)' in inspect.getsource(search_run.SearchRun.discard)
    assert 'request_cancel' in inspect.getsource(search_run._ask_to_stop)
